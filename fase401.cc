#include <vector>
#include <string>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <map>

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/csma-module.h"
#include "ns3/applications-module.h"
#include "ns3/traffic-control-module.h"
#include "ns3/random-variable-stream.h"

#include "punto.h" // Cabecera con las clases Punto, Curva y Grafica

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("ProyectoMEC");

// --- 1. ESTRUCTURAS DE DATOS (Capa de Modelo) ---
struct ResultadosFlujo {
    double delayMedio = 0;
    double jitterMedio = 0;
    double pdr = 0;
    double throughput = 0;
};

struct ResultadosEscenario {
    ResultadosFlujo cloud; 
    ResultadosFlujo edge;  
    uint32_t dropsTotales = 0;
};

// --- 2. CABECERA PERSONALIZADA (Ingeniería de Software) ---
class AppSeqTsHeader : public Header {
public:
    AppSeqTsHeader() : m_seq(0), m_tsNs(0) {}
    void SetSeq(uint32_t s) { m_seq = s; }
    void SetTs(Time t) { m_tsNs = (uint64_t)t.GetNanoSeconds(); }
    Time GetTs() const { return NanoSeconds((int64_t)m_tsNs); }
    static TypeId GetTypeId() {
        static TypeId tid = TypeId("AppSeqTsHeader").SetParent<Header>().AddConstructor<AppSeqTsHeader>();
        return tid;
    }
    TypeId GetInstanceTypeId() const override { return GetTypeId(); }
    uint32_t GetSerializedSize() const override { return 12; }
    void Serialize(Buffer::Iterator i) const override { i.WriteHtonU32(m_seq); i.WriteHtonU64(m_tsNs); }
    uint32_t Deserialize(Buffer::Iterator i) override { m_seq = i.ReadNtohU32(); m_tsNs = i.ReadNtohU64(); return 12; }
    void Print(std::ostream &os) const override { os << "seq=" << m_seq; }
private:
    uint32_t m_seq; uint64_t m_tsNs;
};

// --- 3. OBSERVADOR QoS (Patrón Observer) ---
class ObservadorQos {
public:
    struct Metrics {
        uint64_t rxPkts = 0; uint64_t rxBytes = 0;
        double sumDelay = 0; double lastDelay = 0; double sumJitter = 0;
        Time firstRx = Seconds(0); Time lastRx = Seconds(0);
        bool hasFirst = false; bool hasLastDelay = false;
    };
    explicit ObservadorQos(Time tInicio) : m_tInicio(tInicio) {}

    void Rx(Ptr<Socket> sock) {
        Ptr<Packet> p;
        Address from;
        while ((p = sock->RecvFrom(from))) {
            if (Simulator::Now() < m_tInicio) continue; // Descartar transitorio

            AppSeqTsHeader h;
            if (p->GetSize() < h.GetSerializedSize()) continue;
            p->RemoveHeader(h);

            Address addr;
            sock->GetSockName(addr); 
            InetSocketAddress localAddr = InetSocketAddress::ConvertFrom(addr);
            uint16_t port = localAddr.GetPort(); // Identifica si es Cloud o Edge

            double delay = (Simulator::Now() - h.GetTs()).GetSeconds();
            Metrics &m = m_flowStats[port]; 

            m.rxPkts++; 
            m.rxBytes += p->GetSize(); 
            m.sumDelay += delay;
            if (m.hasLastDelay) m.sumJitter += std::fabs(delay - m.lastDelay);
            m.lastDelay = delay; 
            m.hasLastDelay = true;
            if (!m.hasFirst) { m.hasFirst = true; m.firstRx = Simulator::Now(); }
            m.lastRx = Simulator::Now();
        }
    }

    ResultadosFlujo GetStats(uint16_t port, uint64_t txPkts) {
        ResultadosFlujo res;
        if (m_flowStats.find(port) == m_flowStats.end()) return res;
        Metrics &m = m_flowStats[port];
        if (m.rxPkts > 0) {
            res.delayMedio = m.sumDelay / (double)m.rxPkts;
            if (m.rxPkts > 1) res.jitterMedio = m.sumJitter / (double)(m.rxPkts - 1);
            if (txPkts > 0) res.pdr = (double)m.rxPkts / (double)txPkts;
            double dur = (m.lastRx - m.firstRx).GetSeconds();
            if (dur > 0) res.throughput = (m.rxBytes * 8.0) / (dur * 1e6); // En Mbps
        }
        return res;
    }
private:
    Time m_tInicio; std::map<uint16_t, Metrics> m_flowStats;
};

// --- 4. APLICACIÓN EMISORA ---
class TelemSender : public Application {
public:
    TelemSender() : m_seq(0), m_txPkts(0) {}
    void Setup(Address dst, uint16_t port, uint32_t size, Time period) {
        m_dst = dst; m_port = port; m_size = size; m_period = period;
    }
    uint64_t GetTxPkts() const { return m_txPkts; }
private:
    void StartApplication() override {
        m_sock = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
        m_sock->Connect(InetSocketAddress(Ipv4Address::ConvertFrom(m_dst), m_port));
        m_sendEv = Simulator::Schedule(m_period, &TelemSender::Send, this);
    }
    void Send() {
        AppSeqTsHeader h; h.SetSeq(m_seq++); h.SetTs(Simulator::Now());
        Ptr<Packet> p = Create<Packet>(m_size); p->AddHeader(h);
        m_sock->Send(p); m_txPkts++;
        m_sendEv = Simulator::Schedule(m_period, &TelemSender::Send, this);
    }
    Ptr<Socket> m_sock; Address m_dst; uint16_t m_port; uint32_t m_size; 
    Time m_period; uint32_t m_seq; uint64_t m_txPkts; EventId m_sendEv;
};

// Callback para Drops de Cola
static void PacketDropCallback(uint32_t *contadorDrops, Ptr<const QueueDiscItem> item) {
    (*contadorDrops)++;
}

// --- 5. LÓGICA DE ESCENARIO M.E.C. ---
static ResultadosEscenario EjecutarEscenario(double backMbps, uint32_t queueLimit, double splitRatio, uint32_t nVeh, uint32_t pkSize, Time period, Time totalT, Time tTrans, uint32_t runId) {
    RngSeedManager::SetRun(runId);
    Ptr<Node> rsu = CreateObject<Node>(), edgeSrv = CreateObject<Node>(), cloudSrv = CreateObject<Node>();
    NodeContainer cars; cars.Create(nVeh);
    
    InternetStackHelper stack;
    NodeContainer allNodes(cars, rsu, edgeSrv, cloudSrv);
    stack.Install(allNodes);

    // --- TOPOLOGÍA FÍSICA ---
    // 1. Enlace Local RSU <-> Edge (Altísima velocidad, sin congestión)
    CsmaHelper edgeCsma; edgeCsma.SetChannelAttribute("DataRate", StringValue("1Gbps"));
    NetDeviceContainer edgeDevs = edgeCsma.Install(NodeContainer(rsu, edgeSrv));

    // 2. Enlace WAN RSU <-> Cloud (Nuestro Cuello de Botella Paramétrico)
    PointToPointHelper ppp; 
    ppp.SetDeviceAttribute("DataRate", StringValue(std::to_string(backMbps) + "Mbps"));
    NetDeviceContainer cloudDevs = ppp.Install(NodeContainer(rsu, cloudSrv));

    // COLA FqCoDel en la RSU hacia el Cloud
    uint32_t dropsEnEstaEjecucion = 0;
    TrafficControlHelper tch;
    std::string maxSizeStr = std::to_string(queueLimit) + "p"; 
    tch.SetRootQueueDisc("ns3::FqCoDelQueueDisc", "MaxSize", StringValue(maxSizeStr));
    QueueDiscContainer qdCont = tch.Install(cloudDevs.Get(0));
    qdCont.Get(0)->TraceConnectWithoutContext("Drop", MakeBoundCallback(&PacketDropCallback, &dropsEnEstaEjecucion));
    
    // 3. Red de Acceso Vehicular (10Gbps para evitar que los coches se saturen antes de llegar a la RSU)
    CsmaHelper v2r; v2r.SetChannelAttribute("DataRate", StringValue("10Gbps"));
    NodeContainer accessNodes(cars); accessNodes.Add(rsu);
    NetDeviceContainer accessDevs = v2r.Install(accessNodes);

    // --- DIRECCIONAMIENTO IP ---
    Ipv4AddressHelper addr;
    addr.SetBase("10.1.1.0", "255.255.255.0"); Ipv4InterfaceContainer accessIfs = addr.Assign(accessDevs);
    addr.SetBase("10.1.2.0", "255.255.255.0"); Ipv4InterfaceContainer edgeIfs = addr.Assign(edgeDevs);
    addr.SetBase("10.1.3.0", "255.255.255.0"); Ipv4InterfaceContainer cloudIfs = addr.Assign(cloudDevs);
    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    Ptr<UniformRandomVariable> u = CreateObject<UniformRandomVariable>();
    uint16_t pCloud = 5000, pEdgeBurst = 5001, pEdgeTelem = 5002;
    ObservadorQos obs(tTrans);
    std::vector<Ptr<TelemSender>> sendersCloud;
    std::vector<Ptr<TelemSender>> sendersEdge;

    // --- ENRUTAMIENTO INTELIGENTE Y RÁFAGAS ---
    for (uint32_t i = 0; i < nVeh; ++i) {
        Ptr<TelemSender> s = CreateObject<TelemSender>();
        
        // Decisión de Split Ratio: El coche decide a qué IP enviar su telemetría
        if (i < nVeh * splitRatio) {
            s->Setup(edgeIfs.GetAddress(1), pEdgeTelem, pkSize, period); // Al Edge
            sendersEdge.push_back(s);
        } else {
            s->Setup(cloudIfs.GetAddress(1), pCloud, pkSize, period); // Al Cloud
            sendersCloud.push_back(s);
        }
        cars.Get(i)->AddApplication(s);
        s->SetStartTime(Seconds(u->GetValue(0, 0.1))); s->SetStopTime(totalT);

        // Las ráfagas (accidentes) siempre se procesan en el Edge por latencia
        OnOffHelper burst("ns3::UdpSocketFactory", InetSocketAddress(edgeIfs.GetAddress(1), pEdgeBurst));
        burst.SetAttribute("DataRate", StringValue("2Mbps"));
        
        Ptr<ParetoRandomVariable> onTime = CreateObject<ParetoRandomVariable>();
        onTime->SetAttribute("Scale", DoubleValue(0.333)); onTime->SetAttribute("Shape", DoubleValue(1.5));
        Ptr<ExponentialRandomVariable> offTime = CreateObject<ExponentialRandomVariable>();
        offTime->SetAttribute("Mean", DoubleValue(10.0));

        burst.SetAttribute("OnTime", PointerValue(onTime));
        burst.SetAttribute("OffTime", PointerValue(offTime));
        ApplicationContainer bApp = burst.Install(cars.Get(i));
        bApp.Start(Seconds(u->GetValue(2.0, 3.0))); bApp.Stop(totalT);
    }

    // --- RECEPTORES ---
    Ptr<Socket> sCloud = Socket::CreateSocket(cloudSrv, UdpSocketFactory::GetTypeId());
    sCloud->Bind(InetSocketAddress(Ipv4Address::GetAny(), pCloud));
    sCloud->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obs));

    Ptr<Socket> sEdgeBurst = Socket::CreateSocket(edgeSrv, UdpSocketFactory::GetTypeId());
    sEdgeBurst->Bind(InetSocketAddress(Ipv4Address::GetAny(), pEdgeBurst));
    sEdgeBurst->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obs));

    Ptr<Socket> sEdgeTelem = Socket::CreateSocket(edgeSrv, UdpSocketFactory::GetTypeId());
    sEdgeTelem->Bind(InetSocketAddress(Ipv4Address::GetAny(), pEdgeTelem));
    sEdgeTelem->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obs));

    Simulator::Stop(totalT + Seconds(1)); Simulator::Run();

    uint64_t txCloud = 0; for(auto &s : sendersCloud) txCloud += s->GetTxPkts();
    uint64_t txEdgeTelem = 0; for(auto &s : sendersEdge) txEdgeTelem += s->GetTxPkts();
    
    ResultadosEscenario res;
    res.cloud = obs.GetStats(pCloud, txCloud);
    res.edge = obs.GetStats(pEdgeTelem, txEdgeTelem); 
    res.dropsTotales = dropsEnEstaEjecucion;
    Simulator::Destroy();
    return res;
}

// --- 6. MAIN (ESTUDIO PARAMÉTRICO DE ARQUITECTURA DISTRIBUIDA) ---
int main(int argc, char *argv[]) {
    uint32_t nMuestras = 5; // Un buen número de réplicas para suavizar la curva
    CommandLine cmd; 
    cmd.AddValue("numMuestras", "Replicas estadisticas", nMuestras); 
    cmd.Parse(argc, argv);

    // ESCENARIO DE ESTRÉS: Fijamos el Cloud a 3 Mbps (Carga total de la flota es ~4.1 Mbps)
    double backhaulCongestionado = 3.0; 
    uint32_t queueLimitFijo = 100;      

    // Barrido del 0% al 100% del tráfico desviado hacia el Edge
    std::vector<double> ratios = {0.0, 0.2, 0.4, 0.6, 0.8, 1.0};

    // Preparamos las 3 Gráficas Maestras
    Grafica gDelay("retardo_final.plt", "Arquitectura MEC: Retardo vs Split Ratio", "Porcentaje de Trafico al Edge (%)", "Retardo Medio (s)");
    Grafica gPdr("pdr_final.plt", "Arquitectura MEC: PDR vs Split Ratio", "Porcentaje de Trafico al Edge (%)", "PDR (%)");
    Grafica gThput("throughput_final.plt", "Arquitectura MEC: Throughput vs Split Ratio", "Porcentaje de Trafico al Edge (%)", "Throughput (Mbps)");

    Curva cDelayCloud("Retardo en Enlace Cloud");
    Curva cDelayEdge("Retardo en Servidor Edge");
    
    Curva cPdrCloud("PDR Hacia Cloud");
    Curva cPdrEdge("PDR Hacia Edge");
    
    Curva cThputCloud("Caudal Absorbido por Cloud");
    Curva cThputEdge("Caudal Absorbido por Edge");

    NS_LOG_UNCOND("Iniciando Estudio Final TFG/TFM...");

    for (double ratio : ratios) {
        Punto pDelayC, pDelayE, pPdrC, pPdrE, pThC, pThE;
        NS_LOG_UNCOND(">>> Evaluando Derivacion al Edge MEC: " << (ratio * 100) << "%");
        
        for (uint32_t m = 0; m < nMuestras; ++m) {
            ResultadosEscenario r = EjecutarEscenario(backhaulCongestionado, queueLimitFijo, ratio, 50, 1024, Seconds(0.1), Seconds(20), Seconds(2), m + 1);
            
            // Lógica condicional estricta para evitar líneas en 0 irreales
            if (ratio < 1.0) {
                pDelayC.Update(r.cloud.delayMedio);
                pPdrC.Update(r.cloud.pdr * 100.0);
            } else {
                pDelayC.Update(0.0); 
                pPdrC.Update(100.0); // Si no se manda nada al Cloud, PDR es óptimo
            }
            pThC.Update(r.cloud.throughput);

            if (ratio > 0.0) {
                pDelayE.Update(r.edge.delayMedio);
                pPdrE.Update(r.edge.pdr * 100.0);
            } else {
                pDelayE.Update(0.0);
                pPdrE.Update(100.0); // Si no se manda nada al Edge, PDR es óptimo
            }
            pThE.Update(r.edge.throughput);
        }
        
        cDelayCloud.Add(ratio * 100.0, pDelayC); cDelayEdge.Add(ratio * 100.0, pDelayE);
        cPdrCloud.Add(ratio * 100.0, pPdrC);     cPdrEdge.Add(ratio * 100.0, pPdrE);
        cThputCloud.Add(ratio * 100.0, pThC);    cThputEdge.Add(ratio * 100.0, pThE);
    }

    gDelay.Add(cDelayCloud); gDelay.Add(cDelayEdge);
    gPdr.Add(cPdrCloud);     gPdr.Add(cPdrEdge);
    gThput.Add(cThputCloud); gThput.Add(cThputEdge); 

    NS_LOG_UNCOND("¡Simulacion exitosa! Ejecuta gnuplot en los archivos .plt generados.");
    return 0;
}
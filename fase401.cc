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

#include "punto.h" // Asegúrate de que este archivo existe en la misma carpeta

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

// --- 3. OBSERVADOR QoS MULTI-FLUJO (Análisis de Teletráfico) ---
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
        if (Simulator::Now() < m_tInicio) continue;

        AppSeqTsHeader h;
        if (p->GetSize() < h.GetSerializedSize()) continue;
        p->RemoveHeader(h);

        // --- SOLUCIÓN AL ERROR ---
        Address addr;
        sock->GetSockName(addr); // Obtenemos la dirección del socket local
        InetSocketAddress localAddr = InetSocketAddress::ConvertFrom(addr);
        uint16_t port = localAddr.GetPort(); // Extraemos el puerto (5000 o 5001)

        double delay = (Simulator::Now() - h.GetTs()).GetSeconds();
        Metrics &m = m_flowStats[port]; // Ahora 'port' sí está declarado
        // -------------------------

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
            res.pdr = (double)m.rxPkts / (double)txPkts;
            double dur = (m.lastRx - m.firstRx).GetSeconds();
            if (dur > 0) res.throughput = (m.rxBytes * 8.0) / (dur * 1e6);
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


// --- OBSERVADOR DE DESCARTES 
static void PacketDropCallback(uint32_t *contadorDrops, Ptr<const QueueDiscItem> item) {
    (*contadorDrops)++;
}

// --- 5. LÓGICA DE ESCENARIO (Cierre Fase 1.3) ---
static ResultadosEscenario EjecutarEscenario(double backMbps, uint32_t queueLimit, double splitRatio, uint32_t nVeh, uint32_t pkSize, Time period, Time totalT, Time tTrans, uint32_t runId) {
    RngSeedManager::SetRun(runId);
    Ptr<Node> rsu = CreateObject<Node>(), edgeSrv = CreateObject<Node>(), cloudSrv = CreateObject<Node>();
    NodeContainer cars; cars.Create(nVeh);
    
    InternetStackHelper stack;
	  NodeContainer allNodes (cars, rsu, edgeSrv, cloudSrv);
	  stack.Install(allNodes);

    CsmaHelper csma; csma.SetChannelAttribute("DataRate", StringValue("1Gbps"));
    NetDeviceContainer edgeDevs = csma.Install(NodeContainer(rsu, edgeSrv));

    PointToPointHelper ppp; 
    ppp.SetDeviceAttribute("DataRate", StringValue(std::to_string(backMbps) + "Mbps"));
    NetDeviceContainer cloudDevs = ppp.Install(NodeContainer(rsu, cloudSrv));

   // Variable local para contar los descartes de esta ejecución
    uint32_t dropsEnEstaEjecucion = 0;

    // --- GESTIÓN DE COLAS: FqCoDel (Teletráfico) ---
    TrafficControlHelper tch;
    // Configuramos FqCoDel y limitamos su capacidad según el parámetro queueLimit
    tch.SetRootQueueDisc("ns3::FqCoDelQueueDisc", "PacketLimit", UintegerValue(queueLimit));
    
    // Instalamos la cola SOLO en el nodo 0 del enlace Cloud (la interfaz de salida de la RSU)
    QueueDiscContainer qdCont = tch.Install(cloudDevs.Get(0));

    // Conectamos nuestro observador (Sniffer) a la traza "Drop" usando un BoundCallback
    qdCont.Get(0)->TraceConnectWithoutContext("Drop", MakeBoundCallback(&PacketDropCallback, &dropsEnEstaEjecucion));
    
    CsmaHelper v2r; v2r.SetChannelAttribute("DataRate", StringValue("100Mbps"));

    Ipv4AddressHelper addr;
    addr.SetBase("10.1.1.0", "255.255.255.0"); Ipv4InterfaceContainer edgeIfs = addr.Assign(edgeDevs);
    addr.SetBase("10.1.2.0", "255.255.255.0"); Ipv4InterfaceContainer cloudIfs = addr.Assign(cloudDevs);
    Ipv4GlobalRoutingHelper::PopulateRoutingTables();

    Ptr<UniformRandomVariable> u = CreateObject<UniformRandomVariable>();
    uint16_t pCloud = 5000, pEdgeBurst = 5001, pEdgeTelem = 5002;
    ObservadorQos obs(tTrans);
    
    std::vector<Ptr<TelemSender>> sendersCloud;
    std::vector<Ptr<TelemSender>> sendersEdge;

    for (uint32_t i = 0; i < nVeh; ++i) {
        v2r.Install(NodeContainer(cars.Get(i), rsu));
        Ptr<TelemSender> s = CreateObject<TelemSender>();
        
        // --- LÓGICA DE ENRUTAMIENTO EDGE vs CLOUD ---
        // Si el coche entra en el porcentaje de splitRatio, envía al Edge. Si no, al Cloud.
        if (i < nVeh * splitRatio) {
            s->Setup(edgeIfs.GetAddress(1), pEdgeTelem, pkSize, period);
            sendersEdge.push_back(s);
        } else {
            s->Setup(cloudIfs.GetAddress(1), pCloud, pkSize, period);
            sendersCloud.push_back(s);
        }
        
        cars.Get(i)->AddApplication(s);
        s->SetStartTime(Seconds(u->GetValue(0, 0.1))); 
        s->SetStopTime(totalT);

        // Las ráfagas SIEMPRE van al Edge (Puerto 5001)
        OnOffHelper burst("ns3::UdpSocketFactory", InetSocketAddress(edgeIfs.GetAddress(1), pEdgeBurst));
        burst.SetAttribute("DataRate", StringValue("2Mbps"));
        
        Ptr<ParetoRandomVariable> onTime = CreateObject<ParetoRandomVariable>();
        onTime->SetAttribute("Scale", DoubleValue(0.333));
        onTime->SetAttribute("Shape", DoubleValue(1.5));

        Ptr<ExponentialRandomVariable> offTime = CreateObject<ExponentialRandomVariable>();
        offTime->SetAttribute("Mean", DoubleValue(10.0));

        burst.SetAttribute("OnTime", PointerValue(onTime));
        burst.SetAttribute("OffTime", PointerValue(offTime));
        
        ApplicationContainer bApp = burst.Install(cars.Get(i));
        bApp.Start(Seconds(u->GetValue(2.0, 3.0))); 
        bApp.Stop(totalT);
    }

    // --- SOCKETS RECEPTORES ---
    Ptr<Socket> sCloud = Socket::CreateSocket(cloudSrv, UdpSocketFactory::GetTypeId());
    sCloud->Bind(InetSocketAddress(Ipv4Address::GetAny(), pCloud));
    sCloud->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obs));

    Ptr<Socket> sEdgeBurst = Socket::CreateSocket(edgeSrv, UdpSocketFactory::GetTypeId());
    sEdgeBurst->Bind(InetSocketAddress(Ipv4Address::GetAny(), pEdgeBurst));
    sEdgeBurst->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obs));

    Ptr<Socket> sEdgeTelem = Socket::CreateSocket(edgeSrv, UdpSocketFactory::GetTypeId());
    sEdgeTelem->Bind(InetSocketAddress(Ipv4Address::GetAny(), pEdgeTelem));
    sEdgeTelem->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obs));

    Simulator::Stop(totalT + Seconds(1)); 
    Simulator::Run();

    // Contabilizamos paquetes enviados para el PDR
    uint64_t txCloud = 0; for(auto &s : sendersCloud) txCloud += s->GetTxPkts();
    uint64_t txEdgeTelem = 0; for(auto &s : sendersEdge) txEdgeTelem += s->GetTxPkts();
    
    // (Omitimos el cálculo teórico de ráfagas para no ensuciar, nos centramos en telemetría)
    ResultadosEscenario res;
    res.cloud = obs.GetStats(pCloud, txCloud);
    // Para simplificar la salida, guardamos la métrica de telemetría del Edge
    res.edge = obs.GetStats(pEdgeTelem, txEdgeTelem); 
    res.dropsTotales = dropsEnEstaEjecucion;
    Simulator::Destroy();
    return res;
}


// --- 6. MAIN (ESTUDIO PARAMÉTRICO: SPLIT RATIO MEC) ---
int main(int argc, char *argv[]) {
    uint32_t nMuestras = 3; 
    CommandLine cmd; 
    cmd.AddValue("numMuestras", "Réplicas estadísticas", nMuestras); 
    cmd.Parse(argc, argv);

    // Condiciones fijas para forzar el cuello de botella
    double backhaulCongestionado = 3.0; // 3 Mbps
    uint32_t queueLimitFijo = 100;      // 100 paquetes de buffer

    // Barrido del 0% al 100% hacia el Edge
    std::vector<double> ratios = {0.0, 0.2, 0.4, 0.6, 0.8, 1.0};

    Grafica gDelay("retardo_split.plt", "Efecto del Edge Computing: Retardo vs Split Ratio", "Porcentaje de Trafico al Edge (%)", "Retardo Medio Cloud (s)");
    Curva cDelay("Retardo Cloud");

    NS_LOG_UNCOND("Iniciando Estudio de Split Ratio (MEC)...");

    for (double ratio : ratios) {
        Punto pDelay;
        NS_LOG_UNCOND(">>> Evaluando Split Ratio: " << (ratio * 100) << "% al Edge");
        
        for (uint32_t m = 0; m < nMuestras; ++m) {
            ResultadosEscenario r = EjecutarEscenario(backhaulCongestionado, queueLimitFijo, ratio, 50, 1024, Seconds(0.1), Seconds(20), Seconds(2), m + 1);
            
            // Si no hay paquetes al Cloud (ratio 1.0), evitamos guardar ceros falsos en el retardo
            if (ratio < 1.0) {
                pDelay.Update(r.cloud.delayMedio);
            } else {
                pDelay.Update(0.0); // Retardo 0 si no hay tráfico
            }
        }
        
        cDelay.Add(ratio * 100.0, pDelay);
    }

    gDelay.Add(cDelay);
    NS_LOG_UNCOND("Simulacion terminada. Grafica retardo_split.plt generada.");
    
    return 0;
}
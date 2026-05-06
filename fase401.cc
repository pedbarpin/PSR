// fase401.cc
//
// Ejemplos de uso
//   ./ns3 run "fase401"
//   ./ns3 run "fase401 --nVehiculos=80 --tamPaqueteBytes=1500 --periodoEnvio=0.05s 
//              --tasaMinMbps=2 --tasaMaxMbps=20 --tasaPasoMbps=2 --numMuestras=10 
//              --duracionSim=30s --tiempoTransitorio=2s"

#include <vector>
#include <string>
#include <cmath>
#include <cstdint>
#include <iostream>

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/traffic-control-module.h"
#include "ns3/random-variable-stream.h"
#include "ns3/rng-seed-manager.h"
#include "ns3/ipv4-global-routing-helper.h"

#include "punto.h"

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("Fase401");

// ---------------------------
// Estructuras de resultados
// ---------------------------
struct ResultadosFlujo {
    double delayMedio = 0;
    double jitterMedio = 0;
    double pdr = 0;
    double throughput = 0;
};

struct ResultadosEscenario {
    ResultadosFlujo cloud; // Datos del puerto 5000
    ResultadosFlujo edge;  // Datos del puerto 5001
    uint32_t dropsTotales = 0;
};
// ---------------------------
// Header simple: seq + timestamp
// ---------------------------
class AppSeqTsHeader : public Header
{
public:
  AppSeqTsHeader() : m_seq(0), m_tsNs(0) {}

  void SetSeq(uint32_t s) { m_seq = s; }
  void SetTs(Time t)      { m_tsNs = (uint64_t) t.GetNanoSeconds(); }

  uint32_t GetSeq() const { return m_seq; }
  Time     GetTs()  const { return NanoSeconds((int64_t)m_tsNs); }

  static TypeId GetTypeId()
  {
    static TypeId tid = TypeId("AppSeqTsHeader")
      .SetParent<Header>()
      .AddConstructor<AppSeqTsHeader>();
    return tid;
  }
  TypeId GetInstanceTypeId() const override { return GetTypeId(); }

  uint32_t GetSerializedSize() const override
  {
    return 4 + 8; // seq(4) + tsNs(8)
  }

  void Serialize(Buffer::Iterator i) const override
  {
    i.WriteHtonU32(m_seq);
    i.WriteHtonU64(m_tsNs);
  }

  uint32_t Deserialize(Buffer::Iterator i) override
  {
    m_seq  = i.ReadNtohU32();
    m_tsNs = i.ReadNtohU64();
    return GetSerializedSize();
  }

  void Print(std::ostream &os) const override
  {
    os << "seq=" << m_seq << " tsNs=" << m_tsNs;
  }

private:
  uint32_t m_seq;
  uint64_t m_tsNs;
};

// ---------------------------
//    Observador de cola
// ---------------------------
class ObservadorCola
{
public:
  explicit ObservadorCola(Ptr<QueueDisc> cola) : m_paquetesPerdidos(0)
  {
    cola->TraceConnectWithoutContext(
      "Drop",
      MakeCallback(&ObservadorCola::PerdidoPaquete, this));
  }

  uint32_t PaquetesPerdidos() const { return m_paquetesPerdidos; }

private:
  void PerdidoPaquete(Ptr<const QueueDiscItem> /*item*/)
  {
    m_paquetesPerdidos += 1;
    NS_LOG_DEBUG("[Cola] Paquete perdido #" << m_paquetesPerdidos);
  }
  uint32_t m_paquetesPerdidos;
};

// ---------------------------
// Observador QoS (sink): delay/jitter/PDR/throughput
// ---------------------------
class ObservadorQos {
public:
    // Estructura interna para separar métricas por puerto (Servicio)
    struct Metrics {
        uint64_t rxPackets = 0;
        uint64_t rxBytes = 0;
        double sumDelay = 0;
        double lastDelay = 0;
        double sumJitter = 0;
        Time firstRxTime = Seconds(0);
        Time lastRxTime = Seconds(0);
        bool hasFirstRx = false;
        bool hasLastDelay = false;
    };

    explicit ObservadorQos(Time tInicio) : m_tInicio(tInicio) {}

    // El Callback de recepción ahora identifica el flujo por el puerto local del socket
    void Rx(Ptr<Socket> sock) {
        Ptr<Packet> p;
        Address from;
        while ((p = sock->RecvFrom(from))) {
            if (Simulator::Now() < m_tInicio) continue;

            // Extraemos cabecera personalizada para latencia E2E[cite: 4]
            AppSeqTsHeader h;
            if (p->GetSize() < h.GetSerializedSize()) continue;
            p->RemoveHeader(h);

            // DETECCIÓN DE SERVICIO: Obtenemos el puerto para saber si es Cloud (5000) o Edge (5001)
            InetSocketAddress localAddr;
            sock->GetSockName(localAddr); 
            uint16_t port = localAddr.GetPort();

            double delay = (Simulator::Now() - h.GetTs()).GetSeconds();
            UpdateFlowMetrics(port, delay, p->GetSize());
        }
    }

    // Devuelve los resultados filtrados por puerto
    ResultadosEscenario GetStats(uint16_t port, uint64_t txPackets) {
        ResultadosEscenario res;
        if (m_flowStats.find(port) == m_flowStats.end()) return res;

        Metrics &m = m_flowStats[port];
        if (m.rxPackets > 0) {
            res.delayMedioSeg = m.sumDelay / (double)m.rxPackets;
            res.jitterMedioSeg = m.sumJitter / (double)(m.rxPackets - 1);
            res.pdr = (double)m.rxPackets / (double)txPackets;
            
            double duracion = (m.lastRxTime - m.firstRxTime).GetSeconds();
            if (duracion > 0) 
                res.throughputMbps = (m.rxBytes * 8.0) / (duracion * 1e6);
        }
        return res;
    }

private:
    Time m_tInicio;
    std::map<uint16_t, Metrics> m_flowStats; // Almacén de métricas por puerto

    void UpdateFlowMetrics(uint16_t port, double delay, uint32_t size) {
        Metrics &m = m_flowStats[port];
        m.rxPackets++;
        m.rxBytes += size;
        m.sumDelay += delay;

        if (m.hasLastDelay) {
            m.sumJitter += std::fabs(delay - m.lastDelay);
        }
        m.lastDelay = delay;
        m.hasLastDelay = true;

        if (!m.hasFirstRx) {
            m.hasFirstRx = true;
            m.firstRxTime = Simulator::Now();
        }
        m.lastRxTime = Simulator::Now();
    }
};
// ---------------------------
// Aplicación emisor (una instancia = un “vehículo”)
// ---------------------------
class TelemSender : public Application
{
public:
  TelemSender() = default;

  void Setup(Address dst, uint16_t port, uint32_t payloadBytes, Time period)
  {
    m_dst = dst;
    m_port = port;
    m_payloadBytes = payloadBytes;
    m_period = period;
  }

  uint64_t GetTxPackets() const { return m_txPackets; }
  uint64_t GetTxBytes() const   { return m_txBytes;   }

private:
  void StartApplication() override
  {
    m_sock = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
    m_sock->Connect(InetSocketAddress(Ipv4Address::ConvertFrom(m_dst), m_port));
    m_seq = 0;
    ScheduleNext();
  }

  void StopApplication() override
  {
    if (m_ev.IsPending()) Simulator::Cancel(m_ev);
    if (m_sock) m_sock->Close();
    m_sock = nullptr;
  }

  void ScheduleNext()
  {
    m_ev = Simulator::Schedule(m_period, &TelemSender::SendOne, this);
  }

  void SendOne()
  {
    // Creamos paquete con header seq+timestamp + payload
    AppSeqTsHeader h;
    h.SetSeq(m_seq++);
    h.SetTs(Simulator::Now());

    Ptr<Packet> p = Create<Packet>(m_payloadBytes);
    p->AddHeader(h);

    m_sock->Send(p);

    m_txPackets++;
    m_txBytes += p->GetSize();

    NS_LOG_DEBUG("[TX] Enviado paquete seq=" << (m_seq-1) << " size=" << p->GetSize() << " bytes");

    ScheduleNext();
  }

private:
  Ptr<Socket> m_sock;
  EventId m_ev;

  Address m_dst;
  uint16_t m_port = 5000;

  uint32_t m_payloadBytes = 1024;
  Time     m_period = Seconds(0.1);

  uint32_t m_seq = 0;

  uint64_t m_txPackets = 0;
  uint64_t m_txBytes   = 0;
};

// ---------------------------
// Ejecuta un escenario (una capacidad concreta)
// ---------------------------
static ResultadosEscenario
EjecutarEscenario(double tasaBackhaulMbps, 
                  uint32_t nVehiculos,
                  uint32_t tamPaqueteBytes,
                  Time periodoEnvio,
                  Time duracionSim,
                  Time tiempoTransitorio,
                  uint32_t runId)
{
  NS_LOG_FUNCTION(tasaBackhaulMbps << nVehiculos);

  // 1. CONFIGURACIÓN DE SEMILLAS Y RÉPLICAS (Estadística PSR)
  RngSeedManager::SetRun(runId); 

  // 2. CREACIÓN DE INFRAESTRUCTURA (Jerarquía MEC)
  Ptr<Node> rsu = CreateObject<Node>();
  Ptr<Node> edgeServer = CreateObject<Node>();  // Servidor MEC local[cite: 1]
  Ptr<Node> cloudServer = CreateObject<Node>(); // Servidor Cloud remoto
  
  NodeContainer carNodes;
  carNodes.Create(nVehiculos);

  InternetStackHelper stack;
  stack.InstallAll(NodeContainer(carNodes, rsu, edgeServer, cloudServer));

  // 3. ENLACES Y TOPOLOGÍA
  // Enlace A: RSU a Servidor Edge (LAN LAN de alta velocidad)[cite: 1]
  CsmaHelper csmaEdge;
  csmaEdge.SetChannelAttribute("DataRate", StringValue("1Gbps"));
  csmaEdge.SetChannelAttribute("Delay", TimeValue(MicroSeconds(10)));
  NetDeviceContainer edgeDevs = csmaEdge.Install(NodeContainer(rsu, edgeServer));

  // Enlace B: RSU a Servidor Cloud (WAN Lenta - El Cuello de Botella)
  PointToPointHelper pppCloud;
  pppEdge.SetDeviceAttribute("DataRate", DataRateValue(DataRate(std::to_string(tasaBackhaulMbps) + "Mbps")));
  pppEdge.SetChannelAttribute("Delay", TimeValue(MilliSeconds(20)));
  NetDeviceContainer cloudDevs = pppCloud.Install(NodeContainer(rsu, cloudServer));

  // Enlace C: Vehículos a RSU (Acceso vehicular simplificado)[cite: 1]
  CsmaHelper v2rsu;
  v2rsu.SetChannelAttribute("DataRate", StringValue("100Mbps"));
  v2rsu.SetChannelAttribute("Delay", MilliSeconds(1));
  
  // 4. DIRECCIONAMIENTO IP (3 Subredes)
  Ipv4AddressHelper address;
  address.SetBase("10.1.1.0", "255.255.255.0"); // Subred Edge
  Ipv4InterfaceContainer edgeIfs = address.Assign(edgeDevs);
  
  address.SetBase("10.1.2.0", "255.255.255.0"); // Subred Cloud
  Ipv4InterfaceContainer cloudIfs = address.Assign(cloudDevs);

  Ipv4GlobalRoutingHelper::PopulateRoutingTables();

  // 5. VARIABLES ALEATORIAS PARA TRÁFICO ESTOCÁSTICO
  Ptr<UniformRandomVariable> u = CreateObject<UniformRandomVariable>();
  u->SetAttribute("Min", DoubleValue(0.0));
  u->SetAttribute("Max", DoubleValue(periodoEnvio.GetSeconds())); 

  // 6. INSTALACIÓN DE APLICACIONES EN CADA VEHÍCULO
  uint16_t portCloud = 5000;
  uint16_t portEdge = 5001;

  for (uint32_t i = 0; i < nVehiculos; ++i) {
      // Conectar coche i a la RSU
      v2rsu.Install(NodeContainer(carNodes.Get(i), rsu));
      
      // A) FLUJO 1: Telemetría Base al CLOUD (CBR)[cite: 4, 7]
      Ptr<TelemSender> baseApp = CreateObject<TelemSender>();
      baseApp->Setup(cloudIfs.GetAddress(1), portCloud, tamPaqueteBytes, periodoEnvio);
      carNodes.Get(i)->AddApplication(baseApp);
      baseApp->SetStartTime(Seconds(u->GetValue()));
      baseApp->SetStopTime(duracionSim);

      // B) FLUJO 2: Emergencia al EDGE (Ráfagas On-Off Pareto)[cite: 7]
      OnOffHelper burstApp("ns3::UdpSocketFactory", 
                           Address(InetSocketAddress(edgeIfs.GetAddress(1), portEdge)));
      burstApp.SetAttribute("DataRate", StringValue("2Mbps"));
      // Pareto para el tiempo ON (Cola pesada = ráfagas densas aleatorias)[cite: 7, 8]
      burstApp.SetOnTime("ns3::ParetoRandomVariable", "Mean", DoubleValue(1.0), "Shape", DoubleValue(1.5));
      burstApp.SetOffTime("ns3::ExponentialRandomVariable", "Mean", DoubleValue(10.0));
      
      ApplicationContainer burstApps = burstApp.Install(carNodes.Get (i));
      burstApps.Start(Seconds(u->GetValue() + 2.0)); 
      burstApps.Stop(duracionSim);
  }

// --- FINAL DE LA FUNCIÓN EjecutarEscenario ---

  // 7. RECEPCIÓN Y MONITORIZACIÓN (Configuración de Sinks)
  // Socket para el Servidor CLOUD (Puerto 5000)
  Ptr<Socket> cloudSink = Socket::CreateSocket(cloudServer, UdpSocketFactory::GetTypeId());
  cloudSink->Bind(InetSocketAddress(Ipv4Address::GetAny(), portCloud));
  cloudSink->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obsQos));

  // Socket para el Servidor EDGE (Puerto 5001)
  Ptr<Socket> edgeSink = Socket::CreateSocket(edgeServer, UdpSocketFactory::GetTypeId());
  edgeSink->Bind(InetSocketAddress(Ipv4Address::GetAny(), portEdge));
  edgeSink->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obsQos));

  // 8. EJECUCIÓN
  Simulator::Stop(duracionSim + Seconds(1.0));
  Simulator::Run();

  // 9. RECOLECCIÓN DE RESULTADOS FINALES (El "Pegamento")
  ResultadosEscenario res;

  // Calculamos paquetes enviados totales (Simplificación: todos los coches envían ambos flujos)
  // En un sistema real, cada aplicación llevaría su propia cuenta de TX
  uint64_t paquetesTxEstimadosPorFlujo = nVehiculos * (duracionSim.GetSeconds() / periodoEnvio.GetSeconds());

  // Extraemos métricas diferenciadas usando los puertos como clave
  res.cloud = obsQos.GetStats(portCloud, paquetesTxEstimadosPorFlujo);
  res.edge  = obsQos.GetStats(portEdge, paquetesTxEstimadosPorFlujo);
  
  // Aquí capturaríamos los drops físicos de la cola si tienes configurado el ObservadorCola
  res.dropsTotales = 0; // Se actualizará en la Fase 2 con el TraceSource de FqCoDel[cite: 4]

  NS_LOG_INFO("Simulación terminada. PDR Cloud: " << res.cloud.pdr << " | PDR Edge: " << res.edge.pdr);

  Simulator::Destroy();
  return res;

}

// ---------------------------
// main: barrido + .plt
// ---------------------------
int main(int argc, char *argv[])
{
  // 1. PARÁMETROS POR LÍNEA DE COMANDOS (Ingeniería de Software)
  uint32_t nVehiculos = 50;
  uint32_t tamPaqueteBytes = 1024;
  double tasaMinMbps = 1.0;
  double tasaMaxMbps = 10.0;
  double tasaPasoMbps = 1.0;
  uint32_t numMuestras = 20; // Réplicas para validez estadística[cite: 4]
  Time duracionSim = Seconds(60.0);
  Time tiempoTransitorio = Seconds(2.0);

  CommandLine cmd;
  cmd.AddValue("nVehiculos", "Carga de tráfico (coches)", nVehiculos);
  cmd.AddValue("tasaMinMbps", "Capacidad mínima Backhaul", tasaMinMbps);
  cmd.AddValue("numMuestras", "Réplicas por punto (PSR)", numMuestras);
  cmd.Parse(argc, argv);

  // 2. CONFIGURACIÓN DE GRÁFICAS (Visualización Analítica)
  Grafica gD("retardo_comparativo.plt", "Retardo Medio: Cloud vs Edge", "Capacidad Backhaul (Mbps)", "Retardo (s)");
  Curva cCloud("Retardo Cloud (P5000)");
  Curva cEdge("Retardo Edge (P5001)");

  uint32_t runId = 1;

  // 3. BUCLE DE EXPERIMENTACIÓN (Estudio Paramétrico)[cite: 3]
  for (double cap = tasaMinMbps; cap <= tasaMaxMbps; cap += tasaPasoMbps)
  {
      NS_LOG_UNCOND(">>> Analizando capacidad Cloud: " << cap << " Mbps");
      Punto pCloud, pEdge; // Acumuladores estadísticos[cite: 4]

      for (uint32_t m = 0; m < numMuestras; ++m)
      {
          ResultadosEscenario r = EjecutarEscenario(cap, nVehiculos, tamPaqueteBytes, 
                                                   Seconds(0.1), duracionSim, 
                                                   tiempoTransitorio, runId++);
          
          pCloud.Update(r.cloud.delayMedio);
          pEdge.Update(r.edge.delayMedio);
      }

      // Añadimos puntos con Intervalo de Confianza[cite: 4]
      cCloud.Add(cap, pCloud);
      cEdge.Add(cap, pEdge);
  }

  gD.Add(cCloud);
  gD.Add(cEdge);

  NS_LOG_UNCOND("Fase 1 Completada. Archivo 'retardo_comparativo.plt' generado.");
  return 0;
}

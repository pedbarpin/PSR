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
struct ResultadosEscenario
{
  double delayMedioSeg      = 0.0;
  double jitterMedioSeg     = 0.0;
  double pdr                = 0.0;
  double throughputMbps     = 0.0;
  uint32_t paquetesPerdidos = 0;   // drops de QueueDisc
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
class ObservadorQos
{
public:
  explicit ObservadorQos(Time tInicio)
    : m_tInicio(tInicio)
  {}

  void Rx(Ptr<Socket> sock)
  {
    Ptr<Packet> p;
    Address from;
    while ((p = sock->RecvFrom(from)))
      {
        if (Simulator::Now() < m_tInicio)
          continue;

        // Extraer header seq+ts
        AppSeqTsHeader h;
        if (p->GetSize() < h.GetSerializedSize())
          continue;

        p->RemoveHeader(h);

        Time tTx = h.GetTs();
        Time now = Simulator::Now();
        double delay = (now - tTx).GetSeconds();

        m_rxPackets++;
        m_rxBytes += p->GetSize(); // payload restante (sin el header)
        m_sumDelay += delay;

        if (m_hasLastDelay)
          m_sumJitter += std::fabs(delay - m_lastDelay);
        else
          m_hasLastDelay = true;

        m_lastDelay = delay;

        NS_LOG_DEBUG("[RX] Paquete seq=" << h.GetSeq() << " delay=" << delay << "s");

        if (!m_hasFirstRx)
          {
            m_hasFirstRx = true;
            m_firstRxTime = now;
          }
        m_lastRxTime = now;
      }
  }

  void SetTxTotals(uint64_t txPkts, uint64_t txBytes)
  {
    m_txPackets = txPkts;
    m_txBytes   = txBytes;
  }

  ResultadosEscenario GetResultados(uint32_t drops) const
  {
    ResultadosEscenario r;
    r.paquetesPerdidos = drops;

    if (m_txPackets == 0) return r;

    if (m_rxPackets > 0)
      {
        r.delayMedioSeg = m_sumDelay / (double)m_rxPackets;
        if (m_rxPackets > 1)
          r.jitterMedioSeg = m_sumJitter / (double)(m_rxPackets - 1);
      }

    r.pdr = (double)m_rxPackets / (double)m_txPackets;

    if (m_hasFirstRx && m_lastRxTime > m_firstRxTime)
      {
        double dur = (m_lastRxTime - m_firstRxTime).GetSeconds();
        r.throughputMbps = (m_rxBytes * 8.0) / (dur * 1e6);
      }

    return r;
  }

private:
  Time m_tInicio;

  // RX stats
  uint64_t m_rxPackets = 0;
  uint64_t m_rxBytes   = 0;

  // TX totals (inyectados desde el emisor)
  uint64_t m_txPackets = 0;
  uint64_t m_txBytes   = 0;

  double m_sumDelay  = 0.0;
  double m_sumJitter = 0.0;
  double m_lastDelay = 0.0;
  bool   m_hasLastDelay = false;

  bool m_hasFirstRx = false;
  Time m_firstRxTime;
  Time m_lastRxTime;
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
                  Time backhaulDelay,
                  uint32_t runId)
{
  NS_LOG_FUNCTION(tasaBackhaulMbps << nVehiculos << tamPaqueteBytes << periodoEnvio << duracionSim);

  // Para réplicas independientes (solo afecta a RNG de desincronización)
  RngSeedManager::SetRun(runId);

  // Nodos: RSU (emisor agregado) + Central (servidor)
  Ptr<Node> rsu = CreateObject<Node>();
  Ptr<Node> central = CreateObject<Node>();
  NodeContainer nodes;
  nodes.Add(rsu);
  nodes.Add(central);

  InternetStackHelper stack;
  stack.SetIpv6StackInstall(false);
  stack.Install(nodes);



  // Enlace backhaul simplificado (PointToPointHelper)
  PointToPointHelper ppp;
  
  // En PPP, la velocidad (DataRate) es un atributo del DISPOSITIVO (Device), no del canal
  ppp.SetDeviceAttribute("DataRate", 
                         DataRateValue(DataRate(std::to_string(tasaBackhaulMbps) + "Mbps")));
  
  // En PPP, el retardo (Delay) es un atributo del CANAL (Channel)
  ppp.SetChannelAttribute("Delay", TimeValue(backhaulDelay));

  // Instalamos los dispositivos
  NetDeviceContainer devs = ppp.Install(nodes);

  // QueueDisc en el netdevice de la RSU (devs.Get(0))
  TrafficControlHelper tch;
  tch.SetRootQueueDisc("ns3::FqCoDelQueueDisc");
  QueueDiscContainer qdiscs = tch.Install(devs.Get(0));
  Ptr<QueueDisc> cola = qdiscs.Get(0);
  ObservadorCola obsCola(cola);

  // IPv4
  Ipv4AddressHelper addr("10.1.1.0", "255.255.255.0");
  Ipv4InterfaceContainer ifs = addr.Assign(devs);
  Ipv4GlobalRoutingHelper::PopulateRoutingTables();

  const uint16_t puerto = 5000;
  Ipv4Address ipCentral = ifs.GetAddress(1); // central es nodo 1

  // Socket RX en Central
  Ptr<Socket> rxSock = Socket::CreateSocket(central, UdpSocketFactory::GetTypeId());
  rxSock->Bind(InetSocketAddress(Ipv4Address::GetAny(), puerto));

  ObservadorQos obsQos(tiempoTransitorio);
  rxSock->SetRecvCallback(MakeCallback(&ObservadorQos::Rx, &obsQos));

// 1. Configuramos el modelo de ráfagas (OnOffApplication)
  OnOffHelper emergencyTraffic ("ns3::UdpSocketFactory", 
                             Address (InetSocketAddress (ifs.GetAddress(1), 5001)) // Puerto 5001 para Alertas

// Seteamos la tasa de la ráfaga: ej. 2 Mbps durante el evento de emergencia
  emergencyTraffic.SetAttribute ("DataRate", StringValue ("2Mbps"));
  emergencyTraffic.SetAttribute ("PacketSize", UintegerValue (1024));

// 2. Modelo Estocástico (Argumento de Teletráfico)
// Usamos Pareto para el tiempo 'On' (ráfaga densa) y Constant para el 'Off'
  emergencyTraffic.SetOnTime ("ns3::ParetoRandomVariable", "Mean", DoubleValue (1.0), "Shape", DoubleValue (1.5));
  emergencyTraffic.SetOffTime ("ns3::ConstantRandomVariable", "Value", DoubleValue (30.0)); // Evento raro

  // N “vehículos” = N apps emisoras en RSU
  Ptr<UniformRandomVariable> u = CreateObject<UniformRandomVariable>();
  u->SetAttribute("Min", DoubleValue(0.0));
  u->SetAttribute("Max", DoubleValue(periodoEnvio.GetSeconds())); // desincronización en [0, T]

  std::vector< Ptr<TelemSender> > senders;
  senders.reserve(nVehiculos);

  ApplicationContainer appsEmergencia;

  for (uint32_t i = 0; i < nVehiculos; ++i)
    {
      Ptr<TelemSender> app = CreateObject<TelemSender>();
      app->Setup(ipCentral, puerto, tamPaqueteBytes, periodoEnvio);
      rsu->AddApplication(app);

      Time t0 = Seconds(u->GetValue());
      app->SetStartTime(t0);
      app->SetStopTime(duracionSim);

      senders.push_back(app);

      
      // Instalamos una instancia de ráfagas por cada vehículo simulado
      ApplicationContainer unaApp = emergencyTraffic.Install(rsu); 
      unaApp.Start(t0); // Usamos el mismo desincronizador para que no empiecen todos a la vez
      unaApp.Stop(duracionSim);
      appsEmergencia.Add(unaApp);
    }

    // Receptor para las ráfagas de emergencia (Puerto 5001)
  PacketSinkHelper sink ("ns3::UdpSocketFactory", 
                      InetSocketAddress (Ipv4Address::GetAny (), 5001));
  ApplicationContainer sinkApp = sink.Install (central);
  sinkApp.Start (Seconds (0.0));
  sinkApp.Stop (duracionSim + Seconds (1.0));

  Simulator::Stop(duracionSim + Seconds(1.0));
  Simulator::Run();

  // Acumular TX totales reales (para PDR “de verdad”)
  uint64_t txPkts = 0, txBytes = 0;
  for (auto &a : senders)
    {
      txPkts  += a->GetTxPackets();
      txBytes += a->GetTxBytes();
    }
  obsQos.SetTxTotals(txPkts, txBytes);

  Simulator::Destroy();

  ResultadosEscenario res = obsQos.GetResultados(obsCola.PaquetesPerdidos());
  NS_LOG_INFO("[Escenario] Backhaul=" << tasaBackhaulMbps << "Mbps | "
              << "Delay=" << res.delayMedioSeg << "s | "
              << "Jitter=" << res.jitterMedioSeg << "s | "
              << "PDR=" << res.pdr << " | "
              << "Throughput=" << res.throughputMbps << "Mbps | "
              << "Drops=" << res.paquetesPerdidos);

  return res;
}

// ---------------------------
// main: barrido + .plt
// ---------------------------
int main(int argc, char *argv[])
{
  Time::SetResolution(Time::NS);

  // Defaults
  uint32_t nVehiculos      = 50;
  uint32_t tamPaqueteBytes = 1024;
  Time periodoEnvio        = Seconds(0.1);

  double tasaMinMbps  = 1.0;
  double tasaMaxMbps  = 10.0;
  double tasaPasoMbps = 1.0;

  uint32_t numMuestras = 20;

  Time duracionSim       = Seconds(60.0);
  Time tiempoTransitorio = Seconds(2.0);
  Time backhaulDelay     = MilliSeconds(2);



  CommandLine cmd;
  cmd.AddValue("nVehiculos", "Numero de vehiculos (fuentes) en cobertura [-]", nVehiculos);
  cmd.AddValue("tamPaqueteBytes", "Tamano payload de telemetria (bytes)", tamPaqueteBytes);
  cmd.AddValue("periodoEnvio", "Periodo entre envios (ej: 0.1s)", periodoEnvio);

  cmd.AddValue("tasaMinMbps", "Capacidad minima backhaul (Mb/s)", tasaMinMbps);
  cmd.AddValue("tasaMaxMbps", "Capacidad maxima backhaul (Mb/s)", tasaMaxMbps);
  cmd.AddValue("tasaPasoMbps", "Paso capacidades (Mb/s)", tasaPasoMbps);

  cmd.AddValue("numMuestras", "Numero de replicas por punto", numMuestras);
  cmd.AddValue("duracionSim", "Duracion simulacion (ej: 60s)", duracionSim);
  cmd.AddValue("tiempoTransitorio", "Transitorio a ignorar (ej: 2s)", tiempoTransitorio);
  cmd.AddValue("backhaulDelay", "Retardo base del backhaul (ej: 2ms)", backhaulDelay);

  cmd.Parse(argc, argv);

  NS_LOG_INFO("=== CONFIGURACION ===");
  NS_LOG_INFO("Vehiculos: " << nVehiculos << " | Payload: " << tamPaqueteBytes << " bytes");
  NS_LOG_INFO("Periodo envio: " << periodoEnvio.As(Time::MS) << " | Duracion: " << duracionSim.As(Time::S));
  NS_LOG_INFO("Backhaul: [" << tasaMinMbps << "-" << tasaMaxMbps << "] Mbps, paso " << tasaPasoMbps);
  NS_LOG_INFO("Muestras por punto: " << numMuestras);
  NS_LOG_INFO("====================");

  // Capacidades
  std::vector<double> capacidades;
  for (double c = tasaMinMbps; c <= tasaMaxMbps + 1e-9; c += tasaPasoMbps)
    capacidades.push_back(c);

  // .plt
  Grafica gD("retardo.plt", "Retardo medio vs capacidad backhaul", "Capacidad (Mb/s)", "Retardo (s)");
  Grafica gJ("jitter.plt", "Jitter medio vs capacidad backhaul", "Capacidad (Mb/s)", "Jitter (s)");
  Grafica gP("pdr.plt", "PDR vs capacidad backhaul", "Capacidad (Mb/s)", "PDR");
  Grafica gT("throughput.plt", "Throughput vs capacidad backhaul", "Capacidad (Mb/s)", "Throughput (Mb/s)");

  Curva cD("Retardo");
  Curva cJ("Jitter");
  Curva cP("PDR");
  Curva cT("Throughput");

  uint32_t runId = 1;

  for (double cap : capacidades)
    {
      NS_LOG_INFO("\n>>> Barriendo capacidad: " << cap << " Mbps");

      Punto pD, pJt, pPdr, pTh;

      for (uint32_t m = 0; m < numMuestras; ++m)
        {
          NS_LOG_INFO("  Muestra " << (m+1) << "/" << numMuestras << "...");
          ResultadosEscenario r = EjecutarEscenario(
            cap, nVehiculos, tamPaqueteBytes, periodoEnvio,
            duracionSim, tiempoTransitorio, backhaulDelay, runId);

          pD.Update(r.delayMedioSeg);
          pJt.Update(r.jitterMedioSeg);
          pPdr.Update(r.pdr);
          pTh.Update(r.throughputMbps);

          runId++;
        }

      cD.Add(cap, pD);
      cJ.Add(cap, pJt);
      cP.Add(cap, pPdr);
      cT.Add(cap, pTh);
    }

  gD.Add(cD);
  gJ.Add(cJ);
  gP.Add(cP);
  gT.Add(cT);

  NS_LOG_INFO("\n=== SIMULACION COMPLETADA ===");
  NS_LOG_INFO("Archivos generados: retardo.plt, jitter.plt, pdr.plt, throughput.plt");

  return 0;
}

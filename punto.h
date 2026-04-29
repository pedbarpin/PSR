#include <map>
#include <array>

#include "ns3/stats-module.h"
#include "ns3/average.h"
#include "ns3/gnuplot.h"

#define PRECISION 0.1

using namespace ns3;

// Tabla de la distribución t-student
// Valores para hasta 31 experimentos y
// probabilidades del 50%, 20%, 10%, 5%, 2% y 1%
std::map<double, std::array<double, 30>> t_student = {
  {0.50, {1.0000, 0.8165, 0.7649, 0.7407, 0.7267, 0.7176, 0.7111, 0.7064,
          0.7027, 0.6998, 0.6974, 0.6955, 0.6938, 0.6924, 0.6912, 0.6901,
          0.6892, 0.6884, 0.6876, 0.6870, 0.6864, 0.6858, 0.6853, 0.6848,
          0.6844, 0.6840, 0.6837, 0.6834, 0.6830, 0.6828}},
  {0.20, {3.0777, 1.8856, 1.6377, 1.5332, 1.4759, 1.4398, 1.4149, 1.3968,
          1.3830, 1.3722, 1.3634, 1.3562, 1.3502, 1.3450, 1.3406, 1.3368,
          1.3334, 1.3304, 1.3277, 1.3253, 1.3232, 1.3212, 1.3195, 1.3178,
          1.3163, 1.3150, 1.3137, 1.3125, 1.3114, 1.3104}},
  {0.10, {6.3137, 2.9200, 2.3534, 2.1318, 2.0150, 1.9432, 1.8946, 1.8595,
          1.8331, 1.8125, 1.7959, 1.7823, 1.7709, 1.7613, 1.7531, 1.7459,
          1.7396, 1.7341, 1.7291, 1.7247, 1.7207, 1.7171, 1.7139, 1.7109,
          1.7081, 1.7056, 1.7033, 1.7011, 1.6991, 1.6973}},
  {0.05, {12.7062, 4.3027, 3.1824, 2.7765, 2.5706, 2.4469, 2.3646, 2.306,
          2.2622, 2.2281, 2.2010, 2.1788, 2.1604, 2.1448, 2.1315, 2.1199,
          2.1098, 2.1009, 2.0930, 2.0860, 2.0796, 2.0739, 2.0687, 2.0639,
          2.0595, 2.0555, 2.0518, 2.0484, 2.0452, 2.0423}},
  {0.02, {31.821, 6.9645, 4.5407, 3.7469, 3.3649, 3.1427, 2.9979, 2.8965,
          2.8214, 2.7638, 2.7181, 2.6810, 2.6503, 2.6245, 2.6025, 2.5835,
          2.5669, 2.5524, 2.5395, 2.5280, 2.5176, 2.5083, 2.4999, 2.4922,
          2.4851, 2.4786, 2.4727, 2.4671, 2.4620, 2.4573}},
  {0.01, {63.6559, 9.925, 5.8408, 4.6041, 4.0321, 3.7074, 3.4995, 3.3554,
          3.2498, 3.1693, 3.1058, 3.0545, 3.0123, 2.9768, 2.9467, 2.9208,
          2.8982, 2.8784, 2.8609, 2.8453, 2.8314, 2.8188, 2.8073, 2.7970,
          2.7874, 2.7787, 2.7707, 2.7633, 2.7564, 2.7500}}
};


////////////////////////////////////////////////////////////////////
// Clase para la acumulación de muestras de un experimento aleatorio.
// Permite obtener la media muestral y el intervalo de confianza.
class Punto
{
 public:
  // Construye un nuevo objeto, con cero muestras.
         Punto  ()
         {}
  // Añade una nueva muestra al conjunto
  void   Update (double valor)
  {
    m_valores.Update (valor);
  }
  // Devuelve el valor medio de las muestras acumuladas
  double Valor  ()
  {
    return m_valores.Mean ();
  }
  // Devuelve la anchura del intervalo de confianza en función
  // de las muestras acumuladas
  // Admite como parámetro la precisión deseada para el IC
  double IC     (double precision)
  {
    // Devuelve la amplitud del intervalo de confianza.
    // La entrada en la tabla es el número de experimentos menos 1,
    // siendo el primer elemento (índice 0) el de un grado de libertad.
    return t_student[precision][m_valores.Count () - 2] *
      sqrt (m_valores.Var () / m_valores.Count ());
  }    

 private:
  // Acumulador de las muestras de los experimentos
  Average<double> m_valores;
};


////////////////////////////////////////////////////////////////////
// Clase que genera una curva GnuPlot a partir de un conjunto de puntos.
// Considera error en el eje vertical.
class Curva
{
 public:
  // Constructor que admite como parámetros:
  //   - la etiqueta que identifica la curva
  //   - la precisión deseada para el intervalo de confianza
  //   - el tipo de curva. Por defecto, línea quebrada con puntos
  //     marcados
  //   - la coordenada de error. Por defecto la ordenada.
  Curva (std::string                 etiqueta,
         double                      precision = PRECISION,
         Gnuplot2dDataset::Style     estilo = Gnuplot2dDataset::LINES_POINTS,
         Gnuplot2dDataset::ErrorBars variable_error = Gnuplot2dDataset::Y)
    : curva (etiqueta)
    {
      m_precision = precision;
      curva.SetStyle (estilo);
      curva.SetErrorBars (variable_error);
    }
  // Añade un punto a la curva. Del punto obtiene el valor medio y el IC
  void Add (double abscisa, Punto punto)
  {
    curva.Add (abscisa, punto.Valor (), punto.IC (m_precision));
  }
  // Devuelve la curva generada
  Gnuplot2dDataset GetCurva ()
  {
    return curva;
  }
 private:
  // Objeto GnuPlot para almacenar la información.
  Gnuplot2dDataset curva;
  // Precision del intervalo de confianza
  double m_precision;
};


////////////////////////////////////////////////////////////////////
// Clase que genera una gráfica GnuPlot, formada por un número
// variable de curvas.
class Grafica
{
 public:
  // Constructor que admite los siguientes parámetros
  // - el nombre del fichero donde almacenar la gráfica en
  //   formato GnuPlot
  // - el título de la gráfica
  // - el rótulo del eje de abscisas
  // - el rótulo del eje de ordenadas
  Grafica (std::string fichero,
           std::string titulo,
           std::string rotulo_abscisas,
           std::string rotulo_ordenadas)
    {
      nombre_fichero = fichero;
      grafica.SetTitle (titulo);
      grafica.SetLegend (rotulo_abscisas, rotulo_ordenadas);
    }
  // Destructor. Cuando el objeto se elimina, se vuelca la
  // información al fichero que se indicó en el constructor.
  ~Grafica ()
    {
      std::ofstream fichero (nombre_fichero);
      grafica.GenerateOutput (fichero);
      fichero << "pause -1" << std::endl;
      fichero.close ();
    }
  // Añade una nueva curva a la gráfica
  void Add (Curva curva)
  {
    grafica.AddDataset (curva.GetCurva ());
  }
 private:
  // Objeto GnuPlot para almacenar la información
  Gnuplot grafica;
  // Nombre del fichero donde se almacenará la información
  std::string nombre_fichero;
};

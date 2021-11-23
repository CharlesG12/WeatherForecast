from pyspark.sql import SparkSession, types
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+


data_schema = types.StructType([
    types.StructField('STATION', types.IntegerType(), False),
    types.StructField('DATE', types.DateType(), False),
    types.StructField('LATITUDE', types.DoubleType(), False),
    types.StructField('LONGITUDE', types.DoubleType(), False),
    types.StructField('ELEVATION', types.IntegerType(), False),
    types.StructField('NAME', types.StringType(), False),
    # Mean temperature (.1 Fahrenheit) Missing = 9999.9
    types.StructField('TEMP', types.DoubleType(), False),
    types.StructField('TEMP_ATTRIBUTES', types.IntegerType(), False),
    # DEWP - Mean dew point (.1 Fahrenheit) Missing = 9999.9
    types.StructField('DEWP', types.DoubleType(), False),
    types.StructField('DEWP_ATTRIBUTES', types.IntegerType(), False),
    # SLP - Mean sea level pressure (.1 mb) Missing = 9999.9
    types.StructField('SLP', types.DoubleType(), False),
    types.StructField('SLP_ATTRIBUTES', types.IntegerType(), False),
    # STP - Mean station pressure (.1 mb)  Missing = 9999.9
    types.StructField('STP', types.DoubleType(), False),
    types.StructField('STP_ATTRIBUTES', types.IntegerType(), False),
    # VISIB - Mean visibility (.1 miles) Missing = 999.9
    types.StructField('VISIB', types.DoubleType(), False),
    types.StructField('VISIB_ATTRIBUTES', types.IntegerType(), False),
    # WDSP – Mean wind speed (.1 knots) Missing = 999.9
    types.StructField('WDSP', types.DoubleType(), False),
    types.StructField('WDSP_ATTRIBUTES', types.IntegerType(), False),
    # MXSPD - Maximum sustained wind speed (.1 knots) Missing = 999.9
    types.StructField('MXSPD', types.DoubleType(), False),
    # GUST - Maximum wind gust (.1 knots) Missing = 999.9
    types.StructField('GUST', types.DoubleType(), False),
    # MAX - Maximum temperature (.1 Fahrenheit) Missing = 9999.9
    types.StructField('MAX', types.DoubleType(), False),
    types.StructField('MAX_ATTRIBUTES', types.StringType(), False),
    #  MIN - Minimum temperature (.1 Fahrenheit) Missing = 9999.9
    types.StructField('MIN', types.DoubleType(), False),
    types.StructField('MIN_ATTRIBUTES', types.StringType(), False),
    # PRCP - Precipitation amount (.01 inches) Missing = 99.99
    types.StructField('PRCP', types.DoubleType(), False),
    types.StructField('PRCP_ATTRIBUTES', types.StringType(), False),
    # SNDP - Snow depth (.1 inches) Missing = 999.9
    types.StructField('SNDP', types.DoubleType(), False),
    # FRSHTT – Indicator for occurrence of: Fog, Rain or Drizzle, Snow or Ice Pellets, Hail, Thunder, Tornado/Funnel Cloud
    # Indicators(1=yes, 0=no/not reported) for the occurrence during the day of:
    # Fog('F' - 1st digit).
    # Rain or Drizzle('R' - 2nd digit).
    # Snow or Ice Pellets('S' - 3rd digit).
    # Hail('H' - 4th digit).
    # Thunder('T' - 5th digit).
    # Tornado or Funnel Cloud('T' - 6th digit)
    types.StructField('FRSHTT', types.IntegerType(), False)
])


def main(inputs):
    data = spark.read.csv(inputs, schema=data_schema)


if __name__ == '__main__':
    inputs = sys.argv[1]
    inputs = "data/2021.tar.gz"
    main(inputs)

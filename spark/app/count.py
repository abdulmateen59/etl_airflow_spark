import logging
import sys
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import sql
from pyspark.sql.functions import expr


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    file = sys.argv[1]
    file_exists = sys.argv[2]

    if file_exists == 'True':
        sc = SparkContext()
        sqlContext = sql.SQLContext(sc)
        mysql = sqlContext.read.options(header=True, delimiter='	').csv(file)
        logger.info(f'Total number of rows in mysql csv = {mysql.count()}')
    else:
        logger.info(f'File not found!!!')


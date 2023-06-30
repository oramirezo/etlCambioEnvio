import os
import re
import subprocess
import threading
import datetime
import sys
import time
from core.appTools import appTools
from core.constants import SERVER_LIMIT_DATA_LEN, TO_EMAIL_LIST, LIMIT_LOCAL
from core.storageController import Hive
from core.pySparkTools import pySparkTools
from core.constants import CIFRAS_ERROR_LOG
from pyspark.sql.functions import col, lit, udf, substring, when, sum, concat, count, first,max
from pyspark.sql.types import StringType, IntegerType
import dateutil


@udf(returnType=StringType())
def decodificador_rsua(s):
    """
    (p_string VARCHAR2)
    RETURN VARCHAR2 AS v_string VARCHAR2(32767);
    """
    p_string = str(s)[:32767]

    """
    V_Posicion1 VARCHAR2(10);
    V_Posicion2 VARCHAR2(10);
    V_Posicion3 VARCHAR2(10);
    V_Bandera NUMBER;
    ValorPosicion1 VARCHAR2(32767);
    ValorPosicion2 VARCHAR2(32767);
    ValorPosicion3 VARCHAR2(32767);
    """
    V_Posicion1 = p_string[0]
    V_Posicion2 = p_string[1]
    V_Posicion3 = p_string[2]
    V_Bandera = 1

    if 48 <= ord(V_Posicion1) <= 57:
        ValorPosicion1 = (ord(V_Posicion1) - 48) * 3844
    elif 65 <= ord(V_Posicion1) <= 90:
        ValorPosicion1 = (ord(V_Posicion1) - 55) * 3844
    elif 97 <= ord(V_Posicion1) <= 122:
        ValorPosicion1 = (ord(V_Posicion1) - 61) * 3844
    else:
        # ValorPosicion1 = ord(V_Posicion1)
        V_Bandera = 1

    if 48 <= ord(V_Posicion2) <= 57:
        ValorPosicion2 = (ord(V_Posicion2) - 48) * 62
    elif 65 <= ord(V_Posicion2) <= 90:
        ValorPosicion2 = (ord(V_Posicion2) - 55) * 62
    elif 97 <= ord(V_Posicion2) <= 122:
        ValorPosicion2 = (ord(V_Posicion2) - 61) * 62
    else:
        # ValorPosicion2 = ord(V_Posicion2)
        V_Bandera = 1

    if 48 <= ord(V_Posicion3) <= 57:
        ValorPosicion3 = (ord(V_Posicion3) - 48) * 1
    elif 65 <= ord(V_Posicion3) <= 90:
        ValorPosicion3 = (ord(V_Posicion3) - 55) * 1
    elif 97 <= ord(V_Posicion3) <= 122:
        ValorPosicion3 = (ord(V_Posicion3) - 61) * 1
    else:
        # ValorPosicion3 = ord(V_Posicion3)
        V_Bandera = 1

    if V_Bandera == 0:
        v_string = (ValorPosicion1 + ValorPosicion2 + ValorPosicion3) / 100
    else:
        v_string = 0
    return str(v_string)


@udf(returnType=StringType())
def replace_comma_func(s):
    s = str(s).replace(',', '')
    return s


@udf(returnType=StringType())
def special_func_replace_0353(s):
    s = str(s).replace('0353', '')
    return s


@udf(returnType=StringType())
def special_func_cve_nss(s):
    if s is None:
        return '999999999999999'
    else:
        if (s == 'Null') or (s == 'None') or (s == '    ') or (s == '        ') or (s == '           '):
            return '999999999999999'
        else:
            return s


@udf(returnType=StringType())
def special_func_folio_incapacidad(s):
    # DECODE(
    # dm_Admin.LIMPIACHAR (SUBSTR(RESTO,261,8))
    # s, NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,261,8))) AS FOLIO_INCAPACIDAD
    if (s == 'Null') or (s == 'None') or (s == '        ') or (s == '') or (s is None):
        return '0'
    else:
        return s


@udf(returnType=StringType())
def special_func_fec_movto_incidencia(s):
    # DECODE(
    # dm_Admin.LIMPIACHAR (SUBSTR(RESTO,25,8))
    # s, NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,25,8))) AS FEC_MOVTO_INCIDENCIA
    if (s == 'Null') or (s == 'None') or (s == '        ') or (s == '') or (s is None):
        return '99991231'
    elif s == '00000000':
        return '99991231'
    else:
        return s


@udf(returnType=StringType())
def special_func_fec_ini_desc_info(s):
    try:
        dateutil.parser.parse(s, fuzzy=False)
        is_date = True
    except ValueError:
        is_date = False
    if is_date:
        return s
    else:
        return '19000101'


@udf(returnType=StringType())
def div_10_func(s):
    return float(s) / 10


@udf(returnType=StringType())
def div_100_func(s):
    return float(s) / 100


@udf(returnType=StringType())
def div_100000_func(s):
    return float(s) / 100000


@udf(returnType=StringType())
def div_1000000_func(s):
    return float(s) / 1000000


@udf(returnType=StringType())
def file_name_rsua(s):
    return f'RSUA{s}'


@udf(returnType=StringType())
def trim_func(s):
    original_s = s
    try:
        s = s.lstrip(' ')
        s = s.rstrip(' ')
        return s
    except Exception as e:
        print(f'[error] trim_func. {e}')
        return original_s


@udf(returnType=StringType())
def limpia_char(s):
    original_s = s
    try:
        """
        CREATE OR REPLACE FUNCTION DM_ADMIN.Limpiachar                                                
          (p_string    VARCHAR2)                                                                
        RETURN VARCHAR2                                                                 
        AS                                                                                    
          v_string    VARCHAR2(32767);  
        BEGIN             
          v_string := REPLACE(REPLACE(REPLACE(replace(replace(
          replace(replace(replace(replace(replace(
          replace(replace(replace(replace(trim(p_string),'|'),','),'$',' '),'"'),'_'),'}'),'')),'@'),'}'),'     ????'),'????'),'?'), 'yyyymmdd');
          RETURN v_string
        """
        s = s[:32767]
        # del_chars = ['|', ',', '$', ' ', '"', '_', '}', '@', '}', '     ????', '????', '?', 'yyyymmdd']
        s = str(s).replace('$', ' ').replace('_', ' ').replace(chr(92), '')
        del_chars = ['|', ',', '"', '}', '@', '}', '     ????', '????', '?', 'yyyymmdd']
        for c in del_chars:
            s = str(s).replace(c, '')
        s = s.lstrip(' ')
        s = s.rstrip(' ')
        return s
    except Exception as e:
        print(f'[error] limpia_char. {e}')
        return original_s


@udf(returnType=StringType())
def get_number(s):
    original_s = s
    try:
        """
        CREATE OR REPLACE FUNCTION DM_ADMIN.GET_NUMBER 
        (
          CARACTERES IN VARCHAR2 
        ) RETURN NUMBER
        IS 
          l_num NUMBER;
        BEGIN
            l_num := to_number(regexp_replace(CARACTERES, '^[0-9]*\.(^[0-9])+', ''));
            l_num := NVL(l_num,0); -> si es null lo cambia por 0
            return l_num;
        EXCEPTION
          WHEN value_error THEN
            RETURN 0;  
        END GET_NUMBER;"""
        s = re.sub(r'^[0-9]*\.(^[0-9])+', '', s)

        if not s:
            return '0'
        else:
            if s.isnumeric():
                return s
            else:
                return '0'
    except Exception as e:
        print(f'[error] get_number. {e}')
        return original_s


@udf(returnType=StringType())
def decode_99_func(s):
    try:
        """
        DECODE(s), 0, 99, s) AS DEL_IMSS
        """
        if s == '0':
            return '99'
        else:
            return s
    except Exception as e:
        print(f'[error] decode_func. {e}')


@udf(returnType=StringType())
def sub_string_func(s):

    return s


class etl(threading.Thread):
    def __init__(self, start_datetime, start_datetime_etl, start_datetime_proc, C1_PROYECTO_v, C2_NOMBRE_ARCHIVO_v,
                 C3_ESTATUS_v):
        threading.Thread.__init__(self)
        self.start_datetime = start_datetime
        self.start_datetime_etl = start_datetime_etl
        self.start_datetime_proc = start_datetime_proc
        self.C1_PROYECTO_v = C1_PROYECTO_v
        self.C2_NOMBRE_ARCHIVO_v = C2_NOMBRE_ARCHIVO_v
        self.C3_ESTATUS_v = C3_ESTATUS_v

    def run(self):
        os_running = appTools().get_os_system()[0]
        if os_running == 'windows':
            from core.constants import PATH_LOCAL_TEMP_LA_WIN as PATH_LOCAL_TEMP_LA
        else:
            appTools().get_kerberos_ticket()
            from core.constants import PATH_LOCAL_TEMP_LA

        process_name = appTools().get_proc_etl_name()
        print(f'> Running process appEtl for \033[93m{self.C2_NOMBRE_ARCHIVO_v}\033[0m...')
        print(f'> Project: {self.C1_PROYECTO_v} | File name: {self.C2_NOMBRE_ARCHIVO_v} | Status: {self.C3_ESTATUS_v}')

        spark_obj = pySparkTools().sparkSession_creator()
        if not spark_obj:
            e = f'Failure sparkSession is None.'
            print(f'[error] {e}')
            error_id = '2.2'
            error_description = e

            ctrl_cfrs = appTools().cifras_controlError(start_datetime=self.start_datetime,
                                                       start_datetime_proc=self.start_datetime_proc,
                                                       end_datetime_proc=appTools().get_datime_now(),
                                                       error_id=error_id,
                                                       error_description=error_description,
                                                       process_name=process_name,
                                                       des_proceso='Inico de sparkSession', fuente='cobranza')
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            sys.exit()

        # Dataframe principal DM_ADMIN_E_CARGA_CENVIO
        v_Nom_Archivo = str(self.C2_NOMBRE_ARCHIVO_v).split('_')[0]


        DM_ADMIN_E_CARGA_CENVIO = pySparkTools().dataframe_from_file(spark_obj=spark_obj,
                                                                   path=f'{PATH_LOCAL_TEMP_LA}/{self.C2_NOMBRE_ARCHIVO_v}',
                                                                   delimiter=',', header=False)

        if DM_ADMIN_E_CARGA_CENVIO:

            new_colsNames = ['resto']

            DM_ADMIN_E_CARGA_CENVIO = pySparkTools().rename_col(dataframe=DM_ADMIN_E_CARGA_CENVIO,
                                                              new_colsNames=new_colsNames)

            # Caracteristicas del Dataframe
            DM_ADMIN_E_CARGA_CENVIO_LEN_COLS = len(DM_ADMIN_E_CARGA_CENVIO.columns)
            DM_ADMIN_E_CARGA_CENVIO_LEN_ROWS = DM_ADMIN_E_CARGA_CENVIO.count()
            file_spec = os.stat(f'{PATH_LOCAL_TEMP_LA}/{self.C2_NOMBRE_ARCHIVO_v}')
            print(f'[ok] File <\033[93m{self.C2_NOMBRE_ARCHIVO_v}\033[0m> loaded to dataframe '
                  f'DM_ADMIN_E_CARGA_CENVIO successfully!')
            print(f'> Dataframe size: Rows= {DM_ADMIN_E_CARGA_CENVIO_LEN_ROWS}, Cols = {DM_ADMIN_E_CARGA_CENVIO_LEN_COLS}.')
            print(f'> File size: {round(file_spec.st_size / 1024 / 1024, 1)}MB')
            DM_ADMIN_E_CARGA_CENVIO.printSchema()
            DM_ADMIN_E_CARGA_CENVIO.show(20)

            dt_now = appTools().get_datime_now()
            SYS_DATE = dt_now.strftime('%Y-%m-%d %H:%M:%S.%f')
            #print(SYS_DATE)


            if v_Nom_Archivo == 'EACEMEX':
                print('carga archivo txt')
                dataframe_struct = {'CVE_PATRON': 'varchar',
                                    'CVE_MODALIDAD': 'varchar',
                                    'PERIODO': 'varchar',
                                    'CREDITO': 'varchar',
                                    'FEC_MOVTO': 'varchar',
                                    'TIPO_REGISTRO': 'varchar',
                                    'FEC_CAPTURA': 'varchar',
                                    'CVE_DELEGACION': 'varchar',
                                    'CVE_SUBDELEGACION': 'varchar',
                                    'CVE_DELEG_USUARIO': 'varchar',
                                    'CVE_SUBDEL_USUARIO': 'varchar',
                                    'USUARIO': 'varchar',
                                    'HORA_CAPTURA': 'varchar',
                                    'NUM_REG_TXT': 'varchar'
                                    }
                DM_ADMIN_T_CARGA_CENVIO, DM_ADMIN_T_CARGA_CENVIO_SCHEMA = pySparkTools().dataframe_from_struct(
                    spark_obj=spark_obj,
                    dataframe_struct=dataframe_struct)
                if DM_ADMIN_T_CARGA_CENVIO:
                    # Caracteristicas del Dataframe
                    DM_ADMIN_T_CARGA_CENVIO_LEN_COLS = len(DM_ADMIN_T_CARGA_CENVIO.columns)
                    print(f'[ok] New Dataframe DM_ADMIN_T_CARGA_RSUA created successfully!. '
                          f'Cols = {DM_ADMIN_T_CARGA_CENVIO_LEN_COLS}.')
                    DM_ADMIN_T_CARGA_CENVIO.printSchema()
                else:
                    e = 'New Dataframe DM_ADMIN_T_CARGA_CENVIO failed'

                    print(f'[error] {e}')
                    error_id = '2.2'
                    error_description = e

                    ctrl_cfrs = appTools().cifras_controlError(start_datetime=self.start_datetime,
                                                               start_datetime_proc=self.start_datetime_proc,
                                                               end_datetime_proc=appTools().get_datime_now(),
                                                               error_id=error_id,
                                                               error_description=error_description,
                                                               process_name=process_name,
                                                               des_proceso='Creacxion de nuevo dataframe desde una estructura',
                                                               fuente='cobranza')
                    appTools().error_logger(ctrl_cif=ctrl_cfrs)
                    sys.exit()

                V_CONTADOR = 0
                rowValues = []

                try:
                    if os_running == 'windows':
                        DM_ADMIN_E_CARGA_CENVIO = DM_ADMIN_E_CARGA_CENVIO.limit(LIMIT_LOCAL)
                    # DM_ADMIN_E_CARGA_RSUA.show(25, False)
                    if SERVER_LIMIT_DATA_LEN:
                        DM_ADMIN_E_CARGA_CENVIO = DM_ADMIN_E_CARGA_CENVIO.limit(SERVER_LIMIT_DATA_LEN)
                        print(f'> DM_ADMIN_E_CARGA_RSUA limited: {SERVER_LIMIT_DATA_LEN}rows')

                    # iter_dataframe = DM_ADMIN_E_CARGA_RSUA.rdd.toLocalIterator()
                    iter_dataframe = DM_ADMIN_E_CARGA_CENVIO.collect()
                    print(f'[ok] Dataframe \033[95mDM_ADMIN_E_CARGA_CENVIO\033[0m ready to iterate')
                except Exception as e:
                    print(f'[error] Failure to collect dataframe. {e}')
                    sys.exit()


                for r in iter_dataframe:
                    V_CVE_PATRON = str(r['resto'])[0:8]
                    V_CVE_MODALIDAD = str(r['resto'])[8:10]
                    V_PERIODO = str(r['resto'])[10:16]
                    V_CREDITO = str(r['resto'])[16:25]
                    V_FEC_MOVTO = str(r['resto'])[25:35]
                    V_TIPO_REGISTRO = str(r['resto'])[35:37]
                    V_FEC_CAPTURA = str(r['resto'])[37:47]
                    V_CVE_DELEGACION = str(r['resto'])[47:49]
                    V_CVE_SUBDELEGACION = str(r['resto'])[49:51]
                    V_CVE_DELEG_USUARIO = str(r['resto'])[51:53]
                    V_CVE_SUBDEL_USUARIO = str(r['resto'])[53:55]
                    V_USUARIO = str(r['resto'])[55:63]
                    V_HORA_CAPTURA = str(r['resto'])[63:71]


                    V_CONTADOR += 1

                    ord_row_values = [V_CVE_PATRON, V_CVE_MODALIDAD, V_PERIODO, V_CREDITO, V_FEC_MOVTO,
                                      V_TIPO_REGISTRO, V_FEC_CAPTURA, V_CVE_DELEGACION,
                                      V_CVE_SUBDELEGACION, V_CVE_DELEG_USUARIO, V_CVE_SUBDEL_USUARIO, V_USUARIO, V_HORA_CAPTURA,
                                      V_CONTADOR]
                    rowValues.append([str(v) for v in ord_row_values])

                try:
                    newRows = spark_obj.createDataFrame(rowValues, DM_ADMIN_T_CARGA_CENVIO_SCHEMA)
                    DM_ADMIN_T_CARGA_CENVIO = DM_ADMIN_T_CARGA_CENVIO.union(newRows)

                    DM_ADMIN_T_CARGA_CENVIO_LEN_ROWS = DM_ADMIN_T_CARGA_CENVIO.count()
                    DM_ADMIN_T_CARGA_CENVIO_LEN_COLS = len(DM_ADMIN_T_CARGA_CENVIO.columns)
                    print(
                        f'[ok] {len(rowValues)} new rows inserted to Dataframe \033[95mDM_ADMIN_T_CARGA_RSUA\033[0m successfully!. '
                        f'Rows = {DM_ADMIN_T_CARGA_CENVIO_LEN_ROWS}, Cols = {DM_ADMIN_T_CARGA_CENVIO_LEN_COLS}.')
                    DM_ADMIN_T_CARGA_CENVIO.printSchema()
                    DM_ADMIN_T_CARGA_CENVIO.show(20)

                except Exception as e:
                    err = f'Appending Rows data to DM_ADMIN_T_CARGA_CENVIO'
                    print(f'[error] {e}')
                    error_id = '2.2'
                    error_description = e

                    ctrl_cfrs = appTools().cifras_controlError(start_datetime=self.start_datetime,
                                                               start_datetime_proc=self.start_datetime_proc,
                                                               end_datetime_proc=appTools().get_datime_now(),
                                                               error_id=error_id,
                                                               error_description=error_description,
                                                               process_name=process_name,
                                                               des_proceso='Creacion DATAFRAME ', fuente='cobranza')
                    appTools().error_logger(ctrl_cif=ctrl_cfrs)
                    sys.exit()

                DM_ADMIN_T_CARGA_CENVIO = DM_ADMIN_T_CARGA_CENVIO.withColumn("NUM_REG_INT",
                                                                                       col("NUM_REG_TXT").cast('int'))
                print('bandera')
                DM_ADMIN_T_CARGA_CENVIO.show(20)

                DF_CENVIO_GROUP = DM_ADMIN_T_CARGA_CENVIO.groupBy('CVE_DELEGACION', 'PERIODO',
                                                                            'CVE_SUBDELEGACION',
                                                                            'CVE_PATRON', 'CVE_MODALIDAD',
                                                                            'CREDITO', 'FEC_MOVTO',
                                                                            'TIPO_REGISTRO',
                                                                            'FEC_CAPTURA').agg(max("NUM_REG_INT"))

                new_colsNames = ['R_CVE_DELEGACION',
                                 'R_PERIODO',
                                 'R_CVE_SUBDELEGACION',
                                 'R_CVE_PATRON',
                                 'R_CVE_MODALIDAD',
                                 'R_CREDITO',
                                 'R_FEC_MOVTO',
                                 'R_TIPO_REGISTRO',
                                 'R_FEC_CAPTURA',
                                 'NUM_REG_INT_MAX']
                DF_CENVIO_GROUP = pySparkTools().rename_col(dataframe=DF_CENVIO_GROUP,
                                                                 new_colsNames=new_colsNames)


                DF_CENVIO_GROUP = DF_CENVIO_GROUP.withColumn("NUM_REG_TXT_MAX",
                                                                       col("NUM_REG_INT_MAX").cast('string'))
                # DF_CENVIO_GROUP.printSchema()
                # DF_CENVIO_GROUP.show()

                DF_H_TEMP = DM_ADMIN_T_CARGA_CENVIO
                DF_H_TEMP = DF_H_TEMP.withColumn('CVE_DELEGACION_ANT', when((col('CVE_DELEGACION') == '39') &
                                                                            ((col('CVE_SUBDELEGACION') == '11') |
                                                                             (col('CVE_SUBDELEGACION') == '16') |
                                                                             (col('CVE_SUBDELEGACION') == '54')), '35')
                                                 .when((col('CVE_DELEGACION') == '39') &
                                                       ((col('CVE_SUBDELEGACION') == '56') |
                                                        (col('CVE_SUBDELEGACION') == '57')), '36').
                                                 when((col('CVE_DELEGACION') == '40') &
                                                      ((col('CVE_SUBDELEGACION') == '01') |
                                                       (col('CVE_SUBDELEGACION') == '06') |
                                                       (col('CVE_SUBDELEGACION') == '58')), '37')
                                                 .when((col('CVE_DELEGACION') == '40') &
                                                       ((col('CVE_SUBDELEGACION') == '11') |
                                                        (col('CVE_SUBDELEGACION') == '54')), '38')
                                                 .otherwise(col('CVE_DELEGACION'))
                                                 )
                DF_H_TEMP = DF_H_TEMP.withColumn('CVE_SUBDELEGACION_ANT', when(
                    (col('CVE_DELEGACION') == '40') & (col('CVE_SUBDELEGACION') == '58'), '54')
                                                 .otherwise(col('CVE_SUBDELEGACION')))

                DF_H_TEMP = DF_H_TEMP.withColumn('CVE_DELEG_USUARIO_ANT', when((col('CVE_DELEGACION') == '39') &
                                                                            ((col('CVE_SUBDELEGACION') == '11') |
                                                                             (col('CVE_SUBDELEGACION') == '16') |
                                                                             (col('CVE_SUBDELEGACION') == '54')), '35')
                                                 .when((col('CVE_DELEGACION') == '39') &
                                                       ((col('CVE_SUBDELEGACION') == '56') |
                                                        (col('CVE_SUBDELEGACION') == '57')), '36').
                                                 when((col('CVE_DELEGACION') == '40') &
                                                      ((col('CVE_SUBDELEGACION') == '01') |
                                                       (col('CVE_SUBDELEGACION') == '06') |
                                                       (col('CVE_SUBDELEGACION') == '58')), '37')
                                                 .when((col('CVE_DELEGACION') == '40') &
                                                       ((col('CVE_SUBDELEGACION') == '11') |
                                                        (col('CVE_SUBDELEGACION') == '54')), '38')
                                                 .otherwise(col('CVE_DELEGACION'))
                                                 )
                DF_H_TEMP = DF_H_TEMP.withColumn('CVE_SUBDEL_USUARIO_ANT', when(
                    (col('CVE_DELEGACION') == '40') & (col('CVE_SUBDELEGACION') == '58'), '54')
                                                 .otherwise(col('CVE_SUBDELEGACION')))

                # DF_H_TEMP.show()
                DF_H_R_JOIN = DF_H_TEMP.join(DF_CENVIO_GROUP,
                                             (DF_H_TEMP['NUM_REG_TXT'] == DF_CENVIO_GROUP['NUM_REG_TXT_MAX']))

               # DF_H_R_JOIN.show(20)

                v_fecha = str(self.C2_NOMBRE_ARCHIVO_v).split('_')[1]
                v_anio = v_fecha[0:4]
                v_mes = v_fecha[4:6]
                print(v_anio)
                print(v_mes)

                DF_H_R_JOIN= DF_H_R_JOIN.withColumn('CVE_DELEGACION',trim_func( col('CVE_DELEGACION')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('PERIODO', trim_func(col('PERIODO')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('CVE_PATRON', trim_func(col('CVE_PATRON')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('CVE_MODALIDAD', trim_func(col('CVE_MODALIDAD')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('CREDITO', trim_func(col('CREDITO')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('CVE_DELEG_USUARIO', trim_func(col('CVE_DELEG_USUARIO')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('CVE_SUBDEL_USUARIO', trim_func(col('CVE_SUBDEL_USUARIO')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('USUARIO', trim_func(col('USUARIO')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('TIPO_REGISTRO', trim_func(col('TIPO_REGISTRO')))
                DF_H_R_JOIN = DF_H_R_JOIN.withColumn('HORA_CAPTURA', trim_func(col('HORA_CAPTURA')))


                CENVIO_DF_TO_HIVE = DF_H_R_JOIN.select(col('CVE_DELEGACION'), col('CVE_DELEGACION_ANT'),
                                                 col('PERIODO'),
                                                 col('CVE_SUBDELEGACION'),
                                                 col('CVE_SUBDELEGACION_ANT'), col('CVE_PATRON'),
                                                 col('CVE_MODALIDAD'),col('CREDITO'),
                                                 col('FEC_MOVTO'),col('TIPO_REGISTRO'),
                                                       col('FEC_CAPTURA'), lit(SYS_DATE).alias('FEC_CARGA'),
                                                 col('CVE_DELEG_USUARIO'), col('CVE_SUBDEL_USUARIO'),
                                                 col('CVE_DELEG_USUARIO_ANT'),
                                                 col('CVE_SUBDEL_USUARIO_ANT'),col('USUARIO'),
                                                 col('HORA_CAPTURA'),
                                                 substring(col('PERIODO'), 1, 4).alias('ANIO_PERIODO'),
                                                 substring(col('PERIODO'), 5, 2).alias('MES_PERIODO'),
                                                 lit(v_anio).alias('ANIO_CORTE'),
                                                 lit(v_mes).alias('MES_CORTE'))

               # DF_H_R_JOIN.show(20)
                # insert into SUT_SIPA_REGISTRO_PATRONES -> Hive
                new_colsNames = [
                    'cve_delegacion',
                    'cve_delegacion_ant',
                    'periodo',
                    'cve_subdelegacion',
                    'cve_subdelegacion_ant',
                    'cve_patron',
                    'cve_modalidad',
                    'cve_credito',
                    'fec_movto',
                    'fec_captura',
                    'fec_carga',
                    'cve_incidencia_actual',
                    'cve_incidencia_anterior',
                    'usuario',
                    'hora_captura'
                    ,'saldo_credito'
                    ,'anio_periodo',
                     'mes_periodo',
                     'anio_corte',
                     'mes_corte']
                #CENVIO_DF_TO_HIVE = pySparkTools().rename_col(dataframe=DF_H_R_JOIN, new_colsNames=new_colsNames)
                CENVIO_DF_TO_HIVE = CENVIO_DF_TO_HIVE.withColumn('fecha_carga', lit(SYS_DATE))
                CENVIO_DF_TO_HIVE = CENVIO_DF_TO_HIVE.withColumn('fecha_proceso', lit(SYS_DATE))

                CENVIO_DF_TO_HIVE.show(20)
                CENVIO_DF_TO_HIVE.printSchema()
                #print(len(DM_ADMIN_T_CARGA_CENVIO.columns))
                # Insert dataframe to TE stage
                pySparkTools().dataframe_to_te(
                    start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                    file_name=self.C2_NOMBRE_ARCHIVO_v,
                    dataframe=CENVIO_DF_TO_HIVE,
                    db_table_lt='lt_aficobranza.H_COP_CENVIO')

                DM_ADMIN_E_CARGA_CENVIO.unpersist()
                DM_ADMIN_T_CARGA_CENVIO.unpersist()
                DF_CENVIO_GROUP.unpersist()
                DF_H_R_JOIN.unpersist()
                DF_H_TEMP.unpersist()
                CENVIO_DF_TO_HIVE.unpersist()
            else:
                print('carga cifras')
                try:
                    path=f'{PATH_LOCAL_TEMP_LA}/{self.C2_NOMBRE_ARCHIVO_v}'
                    contador = 0
                    vlinea=''
                    with open(path) as archivo:
                        for linea in archivo:

                            if contador == 3:
                              vlinea=linea
                            contador = contador + 1

                    archivo.close()
                except Exception as e:
                    ctrl_cfrs = appTools().cifras_controlError(start_datetime=self.start_datetime,
                                                               start_datetime_proc=self.start_datetime_proc,
                                                               end_datetime_proc=appTools().get_datime_now(),
                                                               error_id='1.0',
                                                               error_description=e,
                                                               process_name=process_name,
                                                               des_proceso='lectuta de archivo de cifras',
                                                               fuente='cobranza')
                    appTools().error_logger(ctrl_cif=ctrl_cfrs)
                    sys.exit()



                vlinea = str(vlinea).replace('REG. ESC EN ARCH   :', '')
                vlinea = str(vlinea).replace('\n', '')
                vlinea = vlinea.lstrip(' ')
                vlinea = vlinea.rstrip(' ')
                registros = vlinea
                print(registros)

                table_name = 'te_aficobranza.D_COBRANZA_MENSUAL_CIFRAS'

                insert_data_query = f"insert into {table_name} values ('{self.C2_NOMBRE_ARCHIVO_v}', " \
                                    f"'{registros}', '', '', " \
                                    f"'', '{SYS_DATE}', " \
                                    f"'{SYS_DATE}')"

                try:
                    print(f'> Query to Hive: {insert_data_query}')
                    status_query_hive = Hive().exec_query(insert_data_query)
                    if not status_query_hive:
                        error_id = '3.1'
                        error_description = f'Hive query: {insert_data_query}'
                    else:
                        error_id = None
                except Exception as e:
                    error_id, error_description = appTools().get_error(error=e)
                finally:
                    if error_id:
                        print('> Sending [error] status to cifras_control...')
                        ctrl_cfrs = appTools().cifras_control(db_table_name=table_name,
                                                              start_datetime=self.start_datetime,
                                                              start_datetime_proc=self.start_datetime_proc,
                                                              end_datetime_proc=appTools().get_datime_now(),
                                                              error_id=error_id,
                                                              error_description=error_description,
                                                              process_name=process_name)
                        appTools().error_logger(ctrl_cif=ctrl_cfrs)
                    else:
                        print('> Sending [ok] status to cifras_control...')
                        ctrl_cfrs = appTools().cifras_control(db_table_name=table_name,
                                                              start_datetime=self.start_datetime,
                                                              start_datetime_proc=self.start_datetime_proc,
                                                              end_datetime_proc=appTools().get_datime_now(),
                                                              error_id='0',
                                                              error_description='',
                                                              process_name=process_name)
                        appTools().error_logger(ctrl_cif=ctrl_cfrs)


               # v_renglon= DM_ADMIN_E_CARGA_CENVIO.collect()[3][0]
               # print(v_renglon)






        else:
            e = f'Failure on dataframe creation: DM_ADMIN_E_CARGA_RSUA'
            print(f'[error] {e}')
            error_id = '2.2'
            error_description = e


            ctrl_cfrs = appTools().cifras_controlError(start_datetime=self.start_datetime,
                                                       start_datetime_proc=self.start_datetime_proc,
                                                       end_datetime_proc=appTools().get_datime_now(),
                                                       error_id=error_id,
                                                       error_description=error_description,
                                                       process_name=process_name,
                                                       des_proceso='paso de datos de txt a dataframe',
                                                       fuente='cobranza')
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            sys.exit()
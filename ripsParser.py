# coding=utf-8

import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import time, datetime
import os
import calendar
import sys
import zipfile

class RIPS:

    dfConsultorio = None
    dfCiudades = None
    dfAC = None
    dfUS = None
    dfAF = None

    columns = ["fecha_atencion", "fecha_nacimiento", "nombre", "sexo", "tipo_documento", "identificacion", "diagnostico", "municipio", "edad"]

    _CODIGO_PRESTADOR = "050010185901"
    _NUMERO_DE_LA_FACTURA = "1"
    _FINALIDAD_CONSULTA = "10"
    _CAUSA_EXTERNA = "15"
    _TIPO_DIAGNOSTICO = "1"
    _CONSULTA_PRIMERA_VEZ = "890283"
    _CONSULTA_SEGUNDA_VEZ = "890383"
    _VALOR_CONSULTA = "1"
    _VALOR_CUOTA_MODERADORA = "0"
    _CODIGO_ENTIDAD_ADMINISTRADORA = "000000"
    _TIPO_USUARIO = "4"
    _CODIGO_DEPARTAMENTO = "05"
    _CODIGO_MUNICIPIO = "001"
    _ZONA_RESIDENCIA_URBANA = "U"
    _ZONA_RESIDENCIA_RURAL = "R"
    _RAZON_SOCIAL = "LUIS FERNANDO GOMEZ URIBE"
    _DOCUMENTO = "8265835"
    _TIPO_DOCUMENTO = "CC"
    _TIPO_PACIENTE = "PARTICULAR"
    _MESES = {
        "01": "ENERO",
        "02": "FEBRERO",
        "03": "MARZO",
        "04": "ABRIL",
        "05": "MAYO",
        "06": "JUNIO",
        "07": "JULIO",
        "08": "AGOSTO",
        "09": "SEPTIEMBRE",
        "10": "OCTUBRE",
        "11": "NOVIEMBRE",
        "12": "DICIEMBRE"
    }

    
    def __init__(self, month, year):
        self.now = pd.Timestamp('now')
        self.month = month
        self.year = year
        self.codigo = "{}{}".format(self.month, self.year)
        self._REPORT_NAME = "UIRIPS{}{}".format(self._MESES[self.month], self._CODIGO_PRESTADOR)
        self.date_range = calendar.monthrange(int(self.year), int(self.month))
    

    def get_age(self, fecha):
        age = relativedelta(datetime.today(), fecha)
        if age.years>0:
            return age.years
        else:
            if age.months>0:
                return age.months
            else:
                return age.days

    def get_age_unit(self, fecha):
        age = relativedelta(datetime.today(), fecha)
        if age.years>0:
            return "1"
        else:
            if age.months>0:
                return "2"
            else:
                return "3"

    def get_document_type(self, row):
        if row.identificacion != "":
            if row.tipo_documento != "":
                if row.tipo_documento == "pasaporte":
                    return "PA"
                return row.tipo_documento
            else:
                if row.edad<7: 
                    return "RC"
                elif row.edad>=18:
                    return "CC"
                else:
                    return "TI"
        else:
            if row.edad<18:
                return "MS"
            else:
                return "AS"
    
    def get_document(self, row):
        if row.identificacion!="":
            return row.identificacion
        else:
            return row.historia

    def get_first_lastname(self, name):
        temp_name = name.split()
        if len(temp_name)==4:
            return temp_name[2]
        else:
            return temp_name[1]

    def get_second_lastname(self, name):
        temp_name = name.split()
        if len(temp_name)==4:
            return temp_name[3]
        elif len(temp_name)==3:
            return temp_name[2]
        else:
            return ""

    def get_first_firstname(self, name):
        temp_name = name.split()
        return temp_name[0]

    def get_second_firstname(self, name):
        temp_name = name.split()
        if len(temp_name)==4:
            return temp_name[1]
        else:
            return ""
    
    def prefix(self, number):
        if number<10:
            return "0{}".format(number)
        else:
            return "{}".format(number)
    
    def generate_exports_folder(self):
        try:
            os.mkdir("exports")
        except:
            pass
        try:
            os.mkdir("exports/.{}".format(self.codigo))
        except:
            pass
    
    def generate_RIPS(self):
        self.generate_exports_folder()
        self.load_report()
        self.load_ciudades()
        self.generate_AC()
        self.generate_US()
        self.generate_AF()
        self.generate_CT()
        self.write_report(self.dfAC, "AC")
        self.write_report(self.dfUS, "US")
        self.write_report(self.dfAF, "AF")
        self.write_report(self.dfCT, "CT")
        self.zip_reports()

    def load_report(self):
        self.dfConsultorio = pd.read_excel("RIPS/{}.xlsx".format(self.codigo), names=self.columns, dtype={"Identificacion": str}, na_values={"nan": ''}, keep_default_na=False)
        self.dfConsultorio["edad"] = self.dfConsultorio.fecha_nacimiento.apply(lambda x: relativedelta(self.now, x).years)
        self.dfConsultorio["municipio"] = self.dfConsultorio["municipio"].apply(lambda x: x.encode('utf-8').upper().replace("á", "A").replace("é", "E").replace("í", "I").replace("ó", "O").replace("ú", "U").replace("ñ", "Ñ"))
        self.dfConsultorio.nombre = self.dfConsultorio.nombre.apply(lambda x: x.upper())

    def load_ciudades(self):
        self.dfCiudades = pd.read_excel("RIPS/ciudades.xlsx", names=["codigo_departamento", "departamento", "codigo_municipio", "municipio"], dtype={"codigo_departamento": str, "codigo_municipio": str})
        self.dfConsultorio = pd.merge(self.dfConsultorio, self.dfCiudades, how="left", on="municipio", validate="many_to_one", indicator=True)
        self.dfConsultorio.loc[self.dfConsultorio["_merge"]=="left_only", "codigo_departamento"] = self._CODIGO_DEPARTAMENTO
        self.dfConsultorio.loc[self.dfConsultorio["_merge"]=="left_only", "codigo_municipio"] = self._CODIGO_MUNICIPIO
    
    def generate_AC(self):
        self.dfAC = pd.DataFrame(columns=["numero_de_la_factura",	"codigo_del_prestador",	"tipo_de_identificacion",	"numero_de_identificacion",	"fecha_de_consulta",	"nro_de_autorizacion",	"codigo_de_la_consulta",	"finalidad_de_la_consulta",	"causa_externa",	"cod_dx_principal",	"cod_dx_rel_1",	"cod_dx_rel_2",	"cod_dx_rel_3",	"tipo_de_diagnostico",	"valor_de_la_consulta",	"valor_cuota_moderadora",	"valor_neto_a_pagar"])
        if self.dfConsultorio.shape[0]>0:
            self.dfAC.tipo_de_identificacion = self.dfConsultorio.apply(self.get_document_type, axis=1)
            self.dfAC.numero_de_identificacion = self.dfConsultorio.apply(self.get_document, axis=1)
            self.dfAC.numero_de_la_factura = self._NUMERO_DE_LA_FACTURA
            self.dfAC.codigo_del_prestador = self._CODIGO_PRESTADOR
            self.dfAC.fecha_de_consulta = self.dfConsultorio.fecha_atencion.apply(lambda x: x.strftime("%d/%m/%Y") )
            self.dfAC.nro_de_autorizacion = ""
            self.dfAC.codigo_de_la_consulta = self._CONSULTA_PRIMERA_VEZ #Este lo voy a dejar asi por el momento, la idea es que dependiendo del tipo de consulta (primera 890283 o segunda 890383 vez uso codigos distintos)
            self.dfAC.finalidad_de_la_consulta = self._FINALIDAD_CONSULTA
            self.dfAC.causa_externa = self._CAUSA_EXTERNA
            self.dfAC.cod_dx_principal = self.dfConsultorio.diagnostico
            self.dfAC.cod_dx_rel_1 = ""
            self.dfAC.cod_dx_rel_2 = ""
            self.dfAC.cod_dx_rel_3 = ""
            self.dfAC.tipo_de_diagnostico = self._TIPO_DIAGNOSTICO
            self.dfAC.valor_de_la_consulta = self._VALOR_CONSULTA
            self.dfAC.valor_cuota_moderadora = self._VALOR_CUOTA_MODERADORA
            self.dfAC.valor_neto_a_pagar = self._VALOR_CONSULTA
    
    def generate_US(self):
        self.dfUS = pd.DataFrame(columns=["tipo_de_identificacion",	"numero_de_identificacion",	"codigo_entidad_administradora", "tipo_de_usuario",	"primer_apellido_del_usuario",	"segundo_apellido_del_usuario",	"primer_nombre_del_usuario",	"segundo_nombre_del_usuario",	"edad",	"unidad_de_medida_de_edad",	"sexo",	"cod_depto",	"cod_mun",	"zona_de_residencia"])
        if self.dfConsultorio.shape[0]>0:
            self.dfUS.tipo_de_identificacion = self.dfAC.tipo_de_identificacion
            self.dfUS.numero_de_identificacion = self.dfAC.numero_de_identificacion
            self.dfUS.codigo_entidad_administradora = self._CODIGO_ENTIDAD_ADMINISTRADORA
            self.dfUS.tipo_de_usuario = self._TIPO_USUARIO
            self.dfUS.primer_apellido_del_usuario = self.dfConsultorio.nombre.apply(self.get_first_lastname)
            self.dfUS.segundo_apellido_del_usuario = self.dfConsultorio.nombre.apply(self.get_second_lastname)
            self.dfUS.primer_nombre_del_usuario = self.dfConsultorio.nombre.apply(self.get_first_firstname)
            self.dfUS.segundo_nombre_del_usuario = self.dfConsultorio.nombre.apply(self.get_second_firstname)
            self.dfUS.edad = self.dfConsultorio.fecha_nacimiento.apply(self.get_age)
            self.dfUS.unidad_de_medida_de_edad = self.dfConsultorio.fecha_nacimiento.apply(self.get_age_unit)
            self.dfUS.sexo = self.dfConsultorio.sexo
            self.dfUS.cod_depto = self.dfConsultorio.codigo_departamento
            self.dfUS.cod_mun = self.dfConsultorio.codigo_municipio
            self.dfUS.zona_de_residencia = self._ZONA_RESIDENCIA_URBANA
            self.dfUS = self.dfUS.drop_duplicates(["tipo_de_identificacion", "numero_de_identificacion"])
    
    def generate_AF(self):
        self.dfAF = pd.DataFrame(columns=["codigo_del_prestador",	"razon_social",	"tipo_de_identificacion",	"numero_de_identificacion",	"numero_de_la_factura",	"fecha_expedicion_de_la_factura",	"fecha_inicial",	"fecha_final",	"codigo_entidad_administradora",	"nombre_entidad_administradora_o_quien_paga_la_factura",	"numero_del_contrato",	"plan_de_beneficios",	"numero_de_la_poliza",	"valor_total_del_copago_y/o_pago_compartido",	"valor_de_la_comision",	"valor_total_de_descuentos",	"valor_neto_a_pagar_por_la_entidad_contratante"])
        self.dfAF.loc[0] = [self._CODIGO_PRESTADOR, self._RAZON_SOCIAL, self._TIPO_DOCUMENTO, self._DOCUMENTO, self._NUMERO_DE_LA_FACTURA, "{}/{}/{}".format(self.prefix(self.date_range[1]), self.month, self.year), "01/{}/{}".format(self.month, self.year), "{}/{}/{}".format(self.prefix(self.date_range[1]), self.month, self.year), self._CODIGO_ENTIDAD_ADMINISTRADORA, self._TIPO_PACIENTE, "", "", "", "", "", "", self._VALOR_CONSULTA]

    def generate_CT(self):
        self.dfCT = pd.DataFrame(columns=["codigo_del_prestador",	"fecha_de_remision",	"codigo_del_archivo",	"total_de_registros",])
        self.dfCT.loc[0] = [self._CODIGO_PRESTADOR, "{}/{}/{}".format(self.prefix(self.date_range[1]), self.month, self.year),"AC%s" % self.codigo, self.dfAC.shape[0]]
        self.dfCT.loc[1] = [self._CODIGO_PRESTADOR, "{}/{}/{}".format(self.prefix(self.date_range[1]), self.month, self.year),"US%s" % self.codigo, self.dfUS.shape[0]]
        self.dfCT.loc[2] = [self._CODIGO_PRESTADOR, "{}/{}/{}".format(self.prefix(self.date_range[1]), self.month, self.year),"AF%s" % self.codigo, self.dfAF.shape[0]]
    
    def write_report(self, df, prefix):
        df.to_csv("exports/.{}/{}{}.txt".format(self.codigo, prefix, self.codigo), encoding='utf-8', index=False, header=False)

    def zip_reports(self):
        src = "exports/.{}".format(self.codigo)
        abs_src = os.path.abspath(src)
        zipf = zipfile.ZipFile("exports/{}.zip".format(self._REPORT_NAME), 'w', zipfile.ZIP_DEFLATED)
        for dirname, subdirs, files in os.walk(src):
            for filename in files:
                absname = os.path.abspath(os.path.join(dirname, filename))
                arcname = absname[len(abs_src) + 1:]
                zipf.write(absname, arcname)
                os.remove(absname)


if __name__== "__main__":
  rips = RIPS(sys.argv[1], sys.argv[2])
  rips.generate_RIPS()
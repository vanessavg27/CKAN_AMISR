#!/usr/bin/env python3.8
import argparse
import datetime
import glob
from jrodb import Api

################################################################################################
#SOLO UNA FECHA
#./send_ckan.py -exp {esf,isr} -type {rti,pow} -date {"YYYY/MM/DD"} 
##########
#RANGO DE FECHAS
#./send_ckan.py -exp {esf,isr} -type {rti,pow} -startdate {"YYYY/MM/DD"} -enddate {"YYYY/MM/DD"}
################################################################################################
#Enviar dia actual
#./send_ckan.py -type rti

#Enviar dia en especifico
#./send_ckan.py -type rti -date "2023/02/27"

#Enviar rango de fecha 
#./send_ckan.py -type rti -startdate "2023/01/01" -enddate "2023/02/28"

class CKAN_AMISR():

    def __init__(self,op):
        self.path      = op.path
        self.day       = op.date_seleccionado
        self.exp       = op.experiment.upper()
        self.startdate = op.daystart
        self.enddate   = op.dayend
        self.datatype  = op.type

        self.files     = []
        self.dictionary= {}
        
        ##############################
        #User's arguments to use CKAN to send AMISR's files.
        ##############################
        url             ='http://10.10.110.250:8085/observatorios/radio-observatorio-jicamarca/database'
        token           ='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJVak4zQnMyekFtOUF0dGw0cnp5UUVWajUxQ3BQTEVMUzRXY1hGUy1vOS1HbkVfWlhmQmxLbXBEend4OUxNOHQtRTdSYzltSHpuVUJqMWNFeSIsImlhdCI6MTY3NjU2MDUzMX0.2aGsmHjud1yP6c4a5-3LYBNHxS3Eg0Tv7KCA1LLa1Qw'
        inst_type       ="Incoherent Scatter Radars"
        owner_org       ='7a52915b-8f79-472a-bffd-229089836040'
        instrument_name ="AMISR-14"
        voc_station_name='Jicamarca'

        self.dictionary["url"]             = url
        self.dictionary["token"]           = token
        self.dictionary["inst_type"]       = inst_type
        self.dictionary["owner_org"]       = owner_org
        self.dictionary["instrument_name"] = instrument_name
        self.dictionary["voc_station_name"]= voc_station_name
        ##############################

        print("DATE        :",self.day)
        print("Experiment  :",self.exp)
        #print("General Path:",self.path)
        
        ##############################
        #Parameter and datatype
        ##############################

        if self.exp == 'ESF' and self.datatype == 'pow':

            self.path  = '/'.join(self.path.split('/')[:-2])+"/data2Fabiano/POWER_H5_AMISR/"

        if self.exp == 'ESF' and self.datatype == 'rti':

            self.path  = self.path
        
        if self.exp == 'ISR' and self.datatype == 'rti':
            pass

        if self.exp == 'ISR' and self.datatype == 'pow':
            pass

        ##############################
        
        #Bandera de rango de fechas.
        if self.startdate != '0' and self.startdate != '0':
            self.flag_date = 1
            self.range_dates()
            #self.numday_variable()
            #self.range_dates()
        else:
            self.flag_date = 0
            self.unique_file()
            #self.numday_variable()
            #self.unique_file()

    def numday_variable(self,variable=[]):
        
        self.dict_variable = {}

        if self.exp == 'ESF' and self.datatype == 'pow':
            
            #ruta                         = "/media/soporte/DATA/data2Fabiano/POWER_H5_AMISR/"
            ruta                          = self.path
            if self.flag_date == 0:
                date                     = datetime.datetime.strptime(self.day,"%Y/%m/%d")
                num_day                  = date.strftime("%Y%j")
                self.dict_variable['file']    = glob.glob("%s%s/%s/%s/"%(ruta,date.year,self.exp.upper()+num_day,"d"+num_day)+"*.hdf5")
                try:
                    self.dict_variable["year_numday"] = self.dict_variable['file'][0].split("/")[-2][1:]
                except IndexError:
                    print("No hay dato *.hdf5 en la ruta: \n","%s%s/%s/%s/"%(ruta,date.year,self.exp.upper()+num_day,"d"+num_day)+"*.hdf5" )
                    #pass
            
            if self.flag_date == 1:

                file =  "%s%s/%s/%s/"%(ruta,variable.year,self.exp.upper()+variable.strftime("%Y%j"),"d"+variable.strftime("%Y%j"))+"*.hdf5"
                self.dict_variable['file']        = glob.glob(file)

                
        if self.exp == 'ESF' and self.datatype == 'rti':

            ruta   = self.path

            if self.flag_date == 0:
                date                     = datetime.datetime.strptime(self.day,"%Y/%m/%d")
                num_day                  = date.strftime("%Y%j")
                self.dict_variable['file']    = glob.glob("%s%s/%s/%s/"%(ruta,date.year,self.exp.upper(),num_day) +"rti_*")
                print("Archivo:    ",self.dict_variable['file'])
                try:
                    self.dict_variable["year_numday"]= self.dict_variable['file'][0].split("/")[-2]
                except IndexError:
                    print("No hay dato *rti en la ruta: \n","%s%s/%s/%s/"%(ruta,date.year,self.exp.upper(),num_day)+"rti_*" )
                    exit()

            if self.flag_date == 1:

                file                  = ruta+"%s/%s/%s/"%(str(variable.year),self.exp.upper(),variable.strftime("%Y%j"))+"rti_*"
                self.dict_variable['file'] = glob.glob(file)
                
                #if len(self.dict_variable['file']) != 0:
                #    self.dict_variable["year_numday"]= self.dict_variable['file'][0].split("/")[-2]

                #print("pritneo",self.dict_variable["year_numday"])
         
        if self.exp == 'ISR' and self.datatype == 'pow':
            pass

        if self.exp == 'ISR' and self.datatype == 'rti':
            pass

        return self.dict_variable

    def unique_file(self):
                
        self.files     = self.numday_variable()['file']
        return self.files

    def range_dates(self):
        
        self.startdate=datetime.datetime.strptime(self.startdate,"%Y/%m/%d")
        self.enddate  =datetime.datetime.strptime(self.enddate,"%Y/%m/%d")
        range_dates   = [self.startdate+datetime.timedelta(days=x) for x in range(0,(self.enddate-self.startdate).days)]
            
        for name in range_dates:

            file=self.numday_variable(variable=name)['file']
            #file =glob.glob(file)

            try:
                self.files.extend(glob.glob(file[0]))
            except IndexError:
                print("No Data:",name.strftime("%Y/%m/%d"),"  ",end="|  ")
                continue
        print(" ")
        print("FILES:",self.files)
        print("Cantidad data",len(self.files))

        try:
            if self.datatype == 'pow':
                self.dict_variable["year_numday"]= self.files[0].split("/")[-2][1:]

            if self.datatype == 'rti':
                self.dict_variable["year_numday"]= self.files[0].split("/")[-2]
            return self.files

        except IndexError:
            print("No hay datos en el rango de fechas:",self.startdate,"y",self.enddate)
            #pass

    def arguments_ckan(self):
        
        if self.exp == 'ESF' or self.exp == 'esf':
            self.exp = "Equatorial Spread F"

        elif self.exp == 'EEJ':
            self.exp = "Electrical ..."

        elif self.exp == 'ISR':
            self.exp = "ISR"

        print("NUMDAY:",self.dict_variable["year_numday"])
        self.dictionary['data_type']=self.datatype.upper()
        print("Nuevo:",self.dict_variable["year_numday"])
        mes_0 = datetime.datetime.strptime(self.dict_variable["year_numday"],"%Y%j")
        
        #Para datos con rango de fechas
        if self.flag_date == 1:

            mes_i = mes_0.month

            for fil in range(len(self.files)):

                if self.datatype == 'rti':
                    mes          = datetime.datetime.strptime(self.files[fil].split("/")[-2],"%Y%j")
                    mes_actual   = datetime.datetime.strptime(self.files[fil].split("/")[-2],"%Y%j").month

                if self.datatype == 'pow':
                    mes          = datetime.datetime.strptime(self.files[fil].split("/")[-2][1:],"%Y%j")
                    mes_actual   = datetime.datetime.strptime(self.files[fil].split("/")[-2][1:],"%Y%j").month


                if fil == 0:                
                    if self.datatype == 'rti':
                        archivos      = [x for x in self.files if datetime.datetime.strptime(x.split("/")[-2],"%Y%j").month == mes_actual]
                        archivos_name = [x.split("/")[-1] for x in archivos]
                        file_date     = [datetime.datetime.strptime(x.split("/")[-2],"%Y%j").strftime("%Y-%m-%d") for x in archivos]
                    if self.datatype == 'pow':
                        archivos      = [x for x in self.files if datetime.datetime.strptime(x.split("/")[-2][1:],"%Y%j").month == mes_actual]
                        archivos_name = [x.split("/")[-1] for x in archivos]
                        file_date     = [datetime.datetime.strptime(x.split("/")[-2][1:],"%Y%j").strftime("%Y-%m-%d") for x in archivos]

                    #print("Archivos",archivos)
                    title         ="%s %s, %s [%s]"%(mes.year,mes.strftime("%B"),self.exp,self.datatype.upper())
                    name          = ("%s-%s-%s-%s"%(mes.year,mes.strftime("%B"),"-".join(self.exp.split(" ")),self.datatype)).lower()

                    self.dictionary['name_dataset']  =name
                    self.dictionary['title_dataset'] =title
                    self.dictionary['path_resources']=archivos
                    self.dictionary['name_resources']=archivos_name
                    self.dictionary['date_resources']=file_date
                    print("\nName:",name)
                    print("Dictionary:",self.dictionary)

                    Arg = self.dictionary
                    self.upfiles_ckan(Arg)

                else:   #Para datos con solo una fecha                
                    if mes_actual != mes_i:
                        mes_i = mes_actual
                        if self.datatype == 'rti':
                            archivos      = [x for x in self.files if datetime.datetime.strptime(x.split("/")[-2],"%Y%j").month == mes_actual]
                            archivos_name = [x.split("/")[-1] for x in archivos]
                            file_date     = [datetime.datetime.strptime(x.split("/")[-2],"%Y%j").strftime("%Y-%m-%d") for x in archivos]
                        #print("Archivos",archivos)
                        if self.datatype == 'pow':
                            archivos      = [x for x in self.files if datetime.datetime.strptime(x.split("/")[-2][1:],"%Y%j").month == mes_actual]
                            archivos_name = [x.split("/")[-1] for x in archivos]
                            file_date     = [datetime.datetime.strptime(x.split("/")[-2][1:],"%Y%j").strftime("%Y-%m-%d") for x in archivos]
                        title         ="%s %s, %s [%s]"%(mes.year,mes.strftime("%B"),self.exp,self.datatype.upper())
                        name           = ("%s-%s-%s-%s"%(mes.year,mes.strftime("%B"),"-".join(self.exp.split(" ")),self.datatype)).lower()

                        self.dictionary['name_dataset']  =name
                        self.dictionary['title_dataset'] =title
                        self.dictionary['path_resources']=archivos
                        self.dictionary['name_resources']=archivos_name
                        self.dictionary['date_resources']=file_date
                    
                        print("\nName:",name)
                        print("Dictionary:",self.dictionary)

                        Arg = self.dictionary     
                        self.upfiles_ckan(Arg)

                    else:
                        continue
        #Para datos con una sola fecha
        else:

            title          = "%s %s, %s [%s]"%(mes_0.year,mes_0.strftime("%B"),self.exp,self.datatype.upper())
            name           = ("%s-%s-%s-%s"%(mes_0.year,mes_0.strftime("%B"),"-".join(self.exp.split(" ")),self.datatype)).lower()
            archivos       = self.files[0]
            archivos_name  = self.files[0].split("/")[-1]
            file_date      = datetime.datetime.strptime(self.dict_variable["year_numday"],"%Y%j").strftime("%Y-%m-%d")
            #title = "2022 January, Equatorial Spread F [RTI]"
            self.dictionary['name_dataset']  =name
            self.dictionary['title_dataset'] =title
            self.dictionary['path_resources']=archivos
            self.dictionary['name_resources']=archivos_name
            self.dictionary['date_resources']=file_date
            
            print()
            print("Titulo:",self.dictionary['title_dataset'])
            #return self.dictionary
            Arg = self.dictionary
            #print("Dictionary:",self.dictionary)
            self.upfiles_ckan(Arg)
            #self.upfiles_ckan(self,self.dictionary)

    def upfiles_ckan(self,Arg):

        with Api(Arg['url'], Authorization=Arg['token']) as access:
            create_dataset = access.create(type_option='dataset',
                                            name=Arg['name_dataset'],
                                            title=Arg['title_dataset'],
                                            owner_org=Arg['owner_org'],
                                            voc_instrument_type=Arg['inst_type'],
                                            data_type=Arg['data_type'],                      
                                            instrument_name=Arg['instrument_name'],
                                            voc_station_name=Arg['voc_station_name'])
            
            print(create_dataset)

            print("ENVIANDO . . .")

            create_resource = access.create(type_option='resource',
                                            package_id=Arg['name_dataset'],
                                            upload=Arg['path_resources'],
                                            name=Arg['name_resources'],
                                            file_date=Arg['date_resources'],
                                            file_type='Image',
                                            #others='key:value',
                                            format="JPEG")
            
            if isinstance(create_resource, dict):
                print("ES INSTANCIA!")
                create_views = access.create(type_option='views', select='dataset', package = create_resource['package'])
                print(create_views)

                print("ENVIADO CON VISOR.")            
            else:
                print("Recurso a enviar: ",create_resource)
                pass

    def send_ckan(self):

        
        self.arguments_ckan()
 
    def edit_ckan(self):
        pass

    def ckan():

        pass

    def delete_resources(self,id_dataset):

        ## PARA ELIMINAR.......
        with Api(self.dictionary['url'], Authorization=self.dictionary['token']) as access:
            #print(access.delete(type_option='dataset', select='purge', package_id='2022 October, esf [RTI]', id=id_dataset))
            
            show = access.show(type_option='dataset', id=id_dataset)
            ids = []
            for u in show['resources']:
                ids.append(u['id'])
            
            print(access.delete(type_option='resource', select='purge', package_id='2022-october-esf-rti', id=ids))

    def delete_dataset(self,id_dataset):

        with Api(self.dictionary['url'], Authorization=self.dictionary['token']) as access:
            print(access.delete(type_option='dataset', select='purge', id=id_dataset))


print( "**** Envio de datos al CKAN ****")
parser = argparse.ArgumentParser()
#datetime.timedelta(days=1) days=1 -> yesterday, days=0 -> today
day = datetime.datetime.now() - datetime.timedelta(days=1)
today = day.strftime("%Y/%m/%d")

##########################    DAY- SELECCION     ################################################################################################
parser.add_argument('-date',action='store',dest='date_seleccionado',help='Seleccionar  fecha si es OFFLINE se ingresa \
					la fecha con el dia deseado. Por defecto, considera el dia anterior',default=today)
##########################  STARTDAY- SELECCION  ################################################################################################
parser.add_argument('-startdate',action='store',dest='daystart',help='Seleccionar fecha si es OFFLINE se ingresa \
					la fecha con el dia deseado. Por defecto, considera el dia anterior',default='0')
##########################   ENDDAY- SELECCION   ################################################################################################
parser.add_argument('-enddate',action='store',dest='dayend',help='Seleccionar fecha si es OFFLINE se ingresa \
					la fecha con el dia deseado. Por defecto, considera el dia anterior',default='0')
##########################       EXPERIMENT      ##############################################################################################
parser.add_argument('-exp',action='store',dest='experiment',help='Tipo de experimento',default='esf')
##########################       DATATYPE        ####################################################################################################
parser.add_argument('-type',action='store',dest='type',help='Tipo de dato ("rti" o "pow")',default='rti')
##########################          PATH         ####################################################################################################
#parser.add_argument('-path',action='store',dest='path',help="Ruta de archivos de datos a enviar",default="/media/soporte/DATA/AMISR14/")
parser.add_argument('-path',action='store',dest='path',help="Ruta de archivos de datos a enviar",default="/mnt/DATA/AMISR14/")
results	   = parser.parse_args()

ob_ckan = CKAN_AMISR(results)
print()
#Para subir archivos
ob_ckan.send_ckan()


#id = "2022-october-esf-rti"
#name= "2022-october-esf-rti"
#ob_ckan.delete_resources(id)
#ob_ckan.delete_dataset(name)
'''
### EDIT ###

Arg={'url': 'http://10.10.110.243:8085/observatorios/radio-observatorio-jicamarca/database', 
     'token': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJVak4zQnMyekFtOUF0dGw0cnp5UUVWajUxQ3BQTEVMUzRXY1hGUy1vOS1HbkVfWlhmQmxLbXBEend4OUxNOHQtRTdSYzltSHpuVUJqMWNFeSIsImlhdCI6MTY3NjU2MDUzMX0.2aGsmHjud1yP6c4a5-3LYBNHxS3Eg0Tv7KCA1LLa1Qw', 
     'owner_org': '7a52915b-8f79-472a-bffd-229089836040',
     'inst_type': 'Incoherent Scatter Radars', 
     'data_type': 'Equatorial Spread F', 
     'instrument_name': 'AMISR-14',
     'voc_station_name': 'Jicamarca', 
     'name_dataset': '2022-october-equatorial-spread-f-rti',
     'title_dataset': '2022 October, Equatorial Spread F [RTI]', 
     'path_resources': ['/home/soporte/Data/AMISR14/2022/ESF/2022276/rti_20221003.jpeg', '/home/soporte/Data/AMISR14/2022/ESF/2022277/rti_20221004.jpeg', '/home/soporte/Data/AMISR14/2022/ESF/2022278/rti_20221005.jpeg', '/home/soporte/Data/AMISR14/2022/ESF/2022279/rti_20221006.jpeg', '/home/soporte/Data/AMISR14/2022/ESF/2022280/rti_20221007.jpeg', '/home/soporte/Data/AMISR14/2022/ESF/2022281/rti_20221008.jpeg'], 
     'name_resources': ['rti_20221003.jpeg', 'rti_20221004.jpeg', 'rti_20221005.jpeg', 'rti_20221006.jpeg', 'rti_20221007.jpeg', 'rti_20221008.jpeg'], 
     'date_resources': ['2022-10-03', '2022-10-04', '2022-10-05', '2022-10-06', '2022-10-07', '2022-10-08']}


with Api(Arg['url'], Authorization=Arg['token']) as access:
    # show = access.show(type_option='dataset', id='2022-october-equatorial-spread-f-rti')
    # ids = []
    # for u in show['resources']:
    #     ids.append(u['id'])
    #     print(access.delete(type_option='resource', select='purge', package_id='2022-september-equatorial-spread-f-rti', id=ids))
    
    patch = access.patch(type_option='dataset', id='2022-october-equatorial-spread-f-rti', data_type='RTI')
    print(patch)

'''

import os 
import sys
from jrodb import Api
import datetime
import argparse
import glob

#id = "2022-october-esf-rti"
#name= "2022-october-esf-rti"
#ob_ckan.delete_resources(id)


## 19/02/2022
class CKAN_AMISR_delete():

    def __init__(self):

        self.dictionary = {}
        
        url             ='http://10.10.110.250:8085/observatorios/radio-observatorio-jicamarca/database'
        token           ='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJVak4zQnMyekFtOUF0dGw0cnp5UUVWajUxQ3BQTEVMUzRXY1hGUy1vOS1HbkVfWlhmQmxLbXBEend4OUxNOHQtRTdSYzltSHpuVUJqMWNFeSIsImlhdCI6MTY3NjU2MDUzMX0.2aGsmHjud1yP6c4a5-3LYBNHxS3Eg0Tv7KCA1LLa1Qw'
        inst_type       ="Incoherent Scatter Radars"
        owner_org       ='7a52915b-8f79-472a-bffd-229089836040'
        instrument_name ="AMISR-14"
        voc_station_name='Jicamarca'
        datatype        ='rti'

        self.dictionary["url"]             = url
        self.dictionary["token"]           = token
        self.dictionary["inst_type"]       = inst_type
        self.dictionary["owner_org"]       = owner_org
        self.dictionary["instrument_name"] = instrument_name
        self.dictionary["voc_station_name"]= voc_station_name
        self.dictionary['data_type']       = datatype

    def delete_function(self,dates,exp):

        if exp == 'ESF' or exp == 'esf':
            exp = "Equatorial Spread F"
        
        datasets = self.dataset_deletes(dates,exp)

        if self.dictionary['data_type'] == 'rti':
            self.files_rti = ["%s_%s.png"%(self.dictionary['data_type'],i.strftime("%Y%m%d") )  for i in self.fechas ]
        

        print("files_rti",self.files_rti)
        
        for n_dataset in datasets:
            print(n_dataset)
            try:
                listas_id_resource,lista_ids = self.function_show(n_dataset,30)
            except:
                print("No existe el dataset:", n_dataset, "en la base de datos CKAN")
                print("%s"%(self.dictionary["url"]))
                continue
            
            print("listas_id_resource",listas_id_resource)
            print("lista_ids",lista_ids)

            for names,ids in zip(listas_id_resource,lista_ids):
                
                for bad_file in self.files_rti:
                
                    if bad_file in names:

                        index_bad =  names.index(bad_file)
                        print("VALUE:",bad_file)
                        print(bad_file,"---",ids[index_bad])
                        print(type(ids[index_bad]))
                        print(n_dataset)
                        self.function_delete(n_dataset, ids[index_bad])

    
    def dataset_deletes(self,dates,exp):
         
        self.fechas   = [ datetime.datetime.strptime(i,"%d-%m-%Y") for i in dates]
        self.años     = [i.year for i in self.fechas]
        self.meses_n  = [i.month for i in self.fechas]
        self.meses_l  = [i.strftime("%B") for i in self.fechas]
        self.dias     = [i.day for i in self.fechas]

        name_id = [("%s-%s-%s-%s"%(self.años[i],self.meses_l[i],"-".join(exp.split(" ")),self.dictionary['data_type'])).lower() for i in range(len(self.años)) ]
        print(self.años)
        print(self.meses_l)
        print(self.meses_n)
        print(self.dias)
        print(name_id)

        return name_id
    
    def function_show(self,dataset_name, n):

        with Api(self.dictionary["url"], Authorization=self.dictionary["token"]) as access:
            
            show = access.show(type_option='dataset', id =dataset_name)
            resources_list = show['resources']
            count = 0
            ids=[]
            self.ids=[]
            for res in resources_list:
                ids.append(res['name'])
                self.ids.append(res['id'])
                count = count + 1
                #print(f'TOTAL: "{count}"')
                output = [ids[i:i + n] for i in range(0, len(ids), n)]
                output_ids = [self.ids[i:i + n] for i in range(0, len(self.ids), n)]
        return output,output_ids 

    def function_delete(self, dataset_name, value):

        with Api(self.dictionary["url"], Authorization=self.dictionary["token"]) as access:
            print('BLOQUE DE: {}'.format(value))
            print(access.delete(type_option='resource', select='purge', package_id=dataset_name, id=[value]))
            print(f'-------------------------')


    def delete_ckan(self):
        pass

class OLD_SERVER():
    
    def __init__(self):
         
        self.user     = "wmaster"
        self.ip       = "jro-app.igp.gob.pe"
        self.password = "XXXXXXXX" #Pedir a soporte la contraseña
        self.port     = 6633

        #ssh -X -C wmaster@jro-app.igp.gob.pe -p 6633

    def command(self,dates,exp):

        self.command = "ssh -X -C %s@%s -p %s "%(self.user,self.ip,self.port)
        
        if exp == "esf":
            exp_path ="ESF_10beams/"
        if exp == "ejj":
            exp_path ="EEJ/"
        
        path = "/home/ftp_users/ftp_amisir/data/JRO/"

        path_c = path+exp_path

        self.fechas      = [ datetime.datetime.strptime(i,"%d-%m-%Y") for i in dates]
        self.dates_files = [i.strftime("%Y%m%d") for i in self.fechas]
        self.años        = [i.year for i in self.fechas]
        self.meses_n     = [i.strftime("%m") for i in self.fechas]
        self.meses_l     = [i.strftime("%B") for i in self.fechas]
        self.dias        = [i.strftime("%d") for i in self.fechas]
        self.files       = ["rti_%s.png"%(i) for i in self.dates_files]
        print("self.dias",self.dias)
        print(self.dates_files)
        print(self.files)
        print(self.años[0],str(self.meses_n[0]),self.dias[0])
        path_c = [path_c+str(self.años[i])+"/"+str(self.meses_n[i])+"/"+str(self.dias[i])+"/"  for i in range(len(dates))]
        
        command_complete = [self.command + "'rm -r %s'"%(i) for i in path_c]
        print(command_complete)

        for exec in command_complete:
            try:
                print(exec)
                #os.system(exec)
            except:
                continue




op2 = OLD_SERVER()
list_esf_10beams = ["19-02-2022","20-02-2022","06-04-2022","04-05-2022","28-07-2022","24-10-2023","27-02-2023","24-08-2023","01-09-2023","16-09-2023","15-01-2023","04-01-2024","25-01-2024","26-01-2024","15-03-2024","16-03-2024","25-08-2023","26-08-2023"]
exp  = 'esf' 
op2.command(list_esf_10beams,exp)

op3 = OLD_SERVER()
exp = 'ejj'
list_ejj = ["04-05-2022","03-05-2022","06-04-2022","08-04-2022","11-04-2022","20-04-2022","21-03-2022","22-03-2022","23-03-2022","24-03-2022","17-03-2022","10-03-2022","20-02-2022","16-10-2021","16-11-2021","05-05-2017","03-11-2016","19-11-2016","02-08-2016","04-07-2016","21-06-2016","20-05-2016","13-05-2016","15-04-2016","04-03-2016","03-03-2016","17-12-2016","30-11-2015"]
op3.command(list_ejj,exp)


exp  = 'esf' 
if exp  == 'esf': 
    op = CKAN_AMISR_delete()
    op.delete_function(list_esf_10beams,exp)
    

#Antes de eliminar datos del servidor, estar seguro si los datos han sido 









from model import Model
from view import View
import traceback
class Controller:


    def __init__(self):
        self.model=Model()
        self.view=View(self)

    def main (self):
        self.view.main()

    def on_botonGuardar_click(self):
        view_user_path_value=self.view.get_start_name_files()
        self.model.set_start_name_files(view_user_path_value)
        #print(view_user_path_value)
        print(f"Valor en model: {self.model.get_start_name_files()}")
         #input_file_path=self.model.principal():

    def on_botonSeleccionarCarpeta_click(self):
        view_folder_virtualization_path=self.view.read_user_path()
        view_folder_virtualization_path+="/"
        self.view.set_user_folder_virtualization_path(view_folder_virtualization_path)
        self.model.set_input_file_path(view_folder_virtualization_path)

       #print(self.view.get_user_folder_virtualization_path())
        print(f"Valor en model: {self.model.get_input_file_path()}")

    def on_botonSeleccionarArchivoUcmdb_click(self):
        view_ucmdb_inventory_path=self.view.read_user_path_files()
        view_ucmdb_inventory_path+=""
        self.view.set_user_file_ucmdb_path(view_ucmdb_inventory_path)

        self.model.set_ucmdb_inventory_path(view_ucmdb_inventory_path)
        print(f"Valor en model: {self.model.get_ucmdb_inventory_path()}")

    def on_botonSeleccionarArchivoSM_click(self):
        view_sm_inventory_path=self.view.read_user_path_files()
        view_sm_inventory_path+=""
        self.view.set_user_file_sm_path(view_sm_inventory_path)

        self.model.set_sm_inventory_path(view_sm_inventory_path)
        print(f"Valor en model: {self.model.get_sm_inventory_path()}")


    def on_botonEjecutar_click(self):
        start_name_files=self.model.get_start_name_files()
        input_file_path=self.model.get_input_file_path()
        ucmdb_inventory_path=self.model.get_ucmdb_inventory_path()
        print(start_name_files)
        print(input_file_path)
        print(ucmdb_inventory_path)
#ENCADENAMIENTO DE EXPECPCIONES
        if ( len(start_name_files)>0 and len(input_file_path)>0 and len(ucmdb_inventory_path)>0 ):
            print("_______________________________________\n")
            try:
                rutaArchivoCruces = self.model.mainModel()
                
                for rutaArchivo in rutaArchivoCruces:
                    self.view.alert_Dialog(f" {rutaArchivo}")
            except KeyError as k:
                #raise KeyError 
                self.view.alert_Dialog(f"ERROR: { str(k)} {type(k)}")
                traceback.print_exc()
            except Exception as e:
                traceback.print_exc()
                self.view.alert_Dialog(f"ERROR: { str(e)} {type(e)}")
            
        else:
            self.view.alert_Dialog("por favor diligencie todos los campos")


if __name__ == '__main__':
    virtualizacion  = Controller()
    virtualizacion.main()


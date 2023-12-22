from Model import Model
from View import View
import traceback

class Controller:
    #Se llama a la capa del modelo y de la vista
    def __init__(self):
        self.model=Model()
        self.view=View(self)
    ##llama metodo main de view para generar la ventana
    def main (self):
        self.view.main()

    def on_botonSeleccionarArchivoTorre_click(self):
        #Extraer ruta dede la vista
        view_torre_inventory_path=self.view.read_user_path_files()
        view_torre_inventory_path+=""
        self.view.set_user_file_enclosure(view_torre_inventory_path)
        #Enviando ruta al model.
        self.model.set_tower_inventory_path(view_torre_inventory_path)
        print(f"Valor en model:{self.model.get_tower_inventory_path()}")
        print(f"Valor en view:{self.view.get_user_file_enclosure()}")

    def on_botonSeleccionarArchivoUcmdb_click(self):
        view_ucmdb_inventory_path=self.view.read_user_path_files()
        view_ucmdb_inventory_path+=""
        self.view.set_user_file_ucmdb_path(view_ucmdb_inventory_path)
        self.model.set_ucmdb_inventory_path(view_ucmdb_inventory_path)
        print(f"El valor en model: {self.model.get_ucmdb_inventory_path()}")
    
    def on_botonSeleccionarArchivoSM_clik(self):
        view_sm_inventory_path=self.view.read_user_path_files()
        view_sm_inventory_path+=""
        self.view.set_user_file_sm_path(view_sm_inventory_path)

        self.model.set_sm_inventory_path(view_sm_inventory_path)
        print(f"Valor en model: {self.model.get_sm_inventory_path()}")

    def on_botonEjecutar_click(self):
        torre_inventory_path=self.model.get_tower_inventory_path()
        ucmdb_inventory_path=self.model.get_ucmdb_inventory_path()
        sm_inventory_path=self.model.get_sm_inventory_path()
        if ( len(torre_inventory_path)>0 and len(ucmdb_inventory_path)>0 and len(sm_inventory_path)>0 ):
            print("_______________________________________\n")
            try:
                rutaArchivoCruces=self.model.mainTest()
                for rutaArchivo in rutaArchivoCruces:
                    self.view.alert_Dialog(f" {rutaArchivo}")
                    
            except Exception as ex:
                traceback.print_exc()
                self.view.alert_Dialog(f"ERROR: { str(ex)} {type(ex)}")
        else:
                self.view.alert_Dialog("por favor diligencie todos los campos")

if __name__ == '__main__':
    enclosure  = Controller()
    enclosure.main()

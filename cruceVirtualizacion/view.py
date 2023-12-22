import tkinter as tk     # from tkinter import Tk for Python 3.x
from tkinter import Button,Entry, Label, ttk,messagebox,filedialog

class View(tk.Tk):

    PAD = 10
    

    def __init__(self,controller):
        super().__init__()
        self.controller = controller
        self.title("Cruce Virtualizacion")            
        self.start_name_files=tk.StringVar()
        self.user_folder_virtualization_path=tk.StringVar()
        self.user_file_ucmdb_path=tk.StringVar()
        self.user_file_sm_path=tk.StringVar()
        self._make_main_frame()
        
        
    def main (self):
        self.mainloop()

    def set_user_file_sm_path(self,user_file_sm_path):
        self.user_file_sm_path.set(user_file_sm_path)

    def set_user_file_ucmdb_path(self,user_file_ucmdb_path):
        self.user_file_ucmdb_path.set(user_file_ucmdb_path)

    def get_user_folder_virtualization_path(self):
        return self.user_folder_virtualization_path.get()

    def set_user_folder_virtualization_path(self,user_folder_path):
        self.user_folder_virtualization_path.set(user_folder_path)

    def get_start_name_files(self):        
        #print(self.user_path_value.get())
        return self.start_name_files.get()

    def _make_main_frame(self):
        #self.main_frm=tk.Tk()
        self.main_frm=tk.Frame(self,width=400,height=400)
        self.main_frm.pack()

        ##start_name_files#####
        cuadroTexto=Entry(self.main_frm,width=50,justify='right',textvariable=self.start_name_files)
        cuadroTexto.grid(row=0,column=1,sticky="e")
        textLabel = Label(self.main_frm,width=50,text="Ingrese la expresion repetida")
        textLabel.grid(row=0,column=0,sticky="e")
        
        botonGuardar=Button(self.main_frm,text="Guardar",command=self.controller.on_botonGuardar_click)
        botonGuardar.grid(row=0,column=2,sticky="e")

        ######input_file_path#####
        botonSeleccionarCarpeta=Button(self.main_frm,text="Seleccione carpeta inv virtualizacion",command=self.controller.on_botonSeleccionarCarpeta_click)
        botonSeleccionarCarpeta.grid(row=2,column=0)
        cuadroFolderTexto=Entry(self.main_frm,width=50,state='readonly',justify='right',textvariable=self.user_folder_virtualization_path)
        cuadroFolderTexto.grid(row=2,column=1,sticky="e")

        #####ucmdb_inventory_path#####
        botonSeleccionarArchivoUcmdb=Button(self.main_frm,text="Seleccione archivo Excel inventario UCMDB",command=self.controller.on_botonSeleccionarArchivoUcmdb_click)
        botonSeleccionarArchivoUcmdb.grid(row=3,column=0)

        cuadroFileTexto=Entry(self.main_frm,width=50,state='readonly',justify='right',textvariable=self.user_file_ucmdb_path)
        cuadroFileTexto.grid(row=3,column=1,sticky="e")

        ##service_manager_path####
        botonSeleccionarArchivoSM=Button(self.main_frm,text="Seleccione archivo Excel inventario ServiceManager",command=self.controller.on_botonSeleccionarArchivoSM_click)
        botonSeleccionarArchivoSM.grid(row=4,column=0)

        cuadroFileTexto=Entry(self.main_frm,width=50,state='readonly',justify='right',textvariable=self.user_file_sm_path)
        cuadroFileTexto.grid(row=4,column=1,sticky="e")

        #EJECUTAR CODIGOS
        botonEjecutar=Button(self.main_frm,text="Ejecutar",command=self.controller.on_botonEjecutar_click)
        botonEjecutar.grid(row=4,column=1)

        botonCruceUCMDBvsTorre=Button(self.main_frm,text="UmcbdVsTorre",command=self.controller.on_botonEjecutar_click)
        botonEjecutar.grid(row=4,column=3)

    def alert_Dialog(self,message):
         messagebox.showinfo("Info!",message ) # título, mensaje

    def read_user_path(self):
        messagebox.showinfo("Info!", "Seleccione la carpeta con los archivos de virtualizacion") # título, mensaje
        filename = filedialog.askdirectory() # show an "Open" dialog box and return the path to the selected file
        return filename

    def read_user_path_files(self): 
        messagebox.showinfo("Info!","Seleccione el inventario Excel de virtualizacion" ) # título, mensaje
        filename = filedialog.askopenfilename() # show an "Open" dialog box and return the path to the selected file
        return filename

    def read_user_text():
        userText=""
        def getTextInput():        
            result=textExample.get(1.0, tk.END+"-1c")
            print(result)
            return result

        root = tk.Tk()
        textExample=tk.Text(root, height=10)
        textExample.pack()
        btnRead=tk.Button(root, height=1, width=10, text="Read", 
                                command=getTextInput)
        btnRead.pack()
        userText=getTextInput()
        root.mainloop()
    #read_user_path("test")
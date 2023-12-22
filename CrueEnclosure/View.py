import tkinter as tk # from tkinter import Tk for Python 3.x
from tkinter import Button,Entry, Label, ttk,messagebox,filedialog

class View(tk.Tk):

    def __init__(self,controller):
        super().__init__()
        self.controller=controller
        self.title("Cruce Enclosure")
        self.user_file_enclosure=tk.StringVar()
        self.user_file_ucmdb_path=tk.StringVar()
        self.user_file_sm_path=tk.StringVar()
        self._make_main_frame()
    
    def main(self):
        self.mainloop()
    #Generando pantalla principal
    def _make_main_frame(self):
        self.main_frm=ttk.Frame(self,width=400,height=800)
        self.main_frm.pack()

        #Label seleccionar Inv Torre
        botonSeleccionarArchivoTorre=Button(self.main_frm,text="Seleccione archivo CSV inventario Torre",command=self.controller.on_botonSeleccionarArchivoTorre_click)
        botonSeleccionarArchivoTorre.grid(row=1,column=0,sticky="e")
        cuadroFileTexto=Entry(self.main_frm,width=50,state='readonly',justify='right',textvariable=self.get_user_file_enclosure())
        cuadroFileTexto.grid(row=1,column=1,sticky="e")

        #Label seleccionar inv UCMDB
        botonSeleccionarArchivoUCMDB=Button(self.main_frm,text="Seleccionar archivo Excel inventario UCMDB",command=self.controller.on_botonSeleccionarArchivoUcmdb_click)
        botonSeleccionarArchivoUCMDB.grid(row=2,column=0,sticky="e")
        cuadroFileTexto=Entry(self.main_frm,width=50,state='readonly',justify='right',textvariable=self.get_user_file_ucmdb_path())
        cuadroFileTexto.grid(row=2,column=1,sticky="e")
        #Label seleccionar inv SM
        botonSeleccionarArchivoSM=Button(self.main_frm,text="Seleccionar archivo CSV inventario Service manager",command=self.controller.on_botonSeleccionarArchivoSM_clik)
        botonSeleccionarArchivoSM.grid(row=3,column=0,sticky="e")
        cuadroFileTexto=Entry(self.main_frm,width=50,state='readonly',justify='right',textvariable=self.get_user_file_sm_path())
        cuadroFileTexto.grid(row=3,column=1)

        #Label Ejecutar codigos
        botonEjecutar=Button(self.main_frm,text="Ejecutar",command=self.controller.on_botonEjecutar_click)
        botonEjecutar.grid(row=4,column=1)

    def alert_Dialog(self,message):
        messagebox.showinfo("Info!",message)
    
    # show an "Open" dialog box and return the path to the selected file
    def read_user_path_files(self):
        messagebox.showinfo("Info!","Seleccione el inventario Excel")
        filename = filedialog.askopenfilename()
        return filename

    #Generando getters y setters
    def set_user_file_sm_path(self,user_file_sm_path):
        self.user_file_sm_path.set(user_file_sm_path)
    def set_user_file_ucmdb_path(self,user_file_ucmdb_path):
        self.user_file_ucmdb_path.set(user_file_ucmdb_path)
    def set_user_file_enclosure(self,user_file_enclosure):
        self.user_file_enclosure.set(user_file_enclosure)
    def get_user_file_sm_path(self):
        return self.user_file_sm_path
    def get_user_file_ucmdb_path(self):
        return self.user_file_ucmdb_path
    def get_user_file_enclosure(self):        
        return self.user_file_enclosure
    

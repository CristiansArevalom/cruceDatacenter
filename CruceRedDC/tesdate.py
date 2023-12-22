from datetime import datetime #Clase
#import datetime as dt #modulo
#import locale
class test:
    def transform_Date(stringDate):
        """#Dada una fecha en formato (jueves 4 de agosto de 2022 23H27' COT) returna la fecha en formato datetime "%A %d de %B de %Y %HH%M"""
        """Dada una fecha en formato Monday, January 23, 2023 12:47:05 PM COT returna la fecha en formato datetime """
        print(repr(stringDate))
        return datetime.strptime(stringDate.replace(" COT",""),"%A, %B %d, %Y %I:%M:%S %p") if (type(stringDate)==str) else stringDate
    
    stringDate="Tuesday, April 11, 2023 7:21:34 PM COT"
    print(transform_Date(stringDate))
    print(type(transform_Date(stringDate)))
    
'''
def transform_Date(stringDate):
    return datetime.strptime(stringDate.replace(" COT",""),"%A, %B %d, %Y %I:%M:%S %p") if (type(stringDate)==str) else stringDate
   ''' 
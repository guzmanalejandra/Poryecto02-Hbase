import time
from datetime import datetime
import pandas as pd
import csv
import os
import random
import numpy as np
import ast

class HbaseSimulator:
    def __init__(self) -> None:
        self.IP = "198.167.0.1"
        self.tables = {}
        self.table_names = self.get_tables()
    # -----------------------------helper functions-----------------------------

    # Function to get all the tables in the HbaseCollections folder
    def get_tables(self):
        directory = "./HbaseCollections"
        all_entries = os.listdir(directory)
        file_names = [entry for entry in all_entries if os.path.isfile(
            os.path.join(directory, entry))]
        file_names = [file_name.replace(".csv", "")
                      for file_name in file_names]
        return file_names

    # Function to get the number of rows in a table
    def count_rows(self, table_name: str) -> int:
        with open(f'./HbaseCollections/{table_name}.csv', 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)
            count = 0
            for row in reader:
                count += 1
        return count

    def check_string_in_file(self, search_string):
        with open('./disabledTables.txt', 'r') as file:
            for line in file:
                if search_string in line:
                    return True
        return False

    def table_exists(self, table_name: str) -> bool:
        if table_name in self.table_names:
            return True
        return False

    # -----------------------------hbase functions-----------------------------

    def get(self, command: str) -> bool:
        command = command.replace("get", "")
        commands = command.split(",")

        if len(commands) != 2:
            print(f"\n=> Hbase::get - Incorrect command format {command}\n")
            return False

        table_name = commands[0].replace(" ", "").replace("'", "")
        row_key = commands[1].replace(" ", "").replace("'", "")

        if not os.path.exists(f"./HbaseCollections/{table_name}.csv"):
            print(f"\n=> Hbase::get - Table {table_name} does not exist.\n")
            return False

        with open(f"./HbaseCollections/{table_name}.csv", 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)
            for row in reader:
                if row[0] == row_key:
                    data = dict(zip(headers, row[1:]))
                    print(f"\n=> Hbase::get - Row Key: {row_key}\n")
                    for key, value in data.items():
                        print(f"{key}: {value}")
                    return True

        print(
            f"\n=> Hbase::get - Row Key {row_key} not found in table {table_name}\n")
        return False

    def scan(self, command: str):
        command = command.replace("scan", "").replace(' ', '').split(",")

        # Checking the command syntax
        if len(command) != 1:
            print(
                f"\nValue error on: {command}\nToo many arguments for scan function.\nUsage: scan '<table_name>'\n"
            )
            return False

        command = command[0].replace("'", "")
        # Checking if the table exists
        if command not in self.table_names:
            print(f"\n=> Hbase::Table - {command} does not exist.\n")
            return False

        # checking if the table is disabled
        if self.check_string_in_file(command):
            print(f"\n=> Hbase::Table - {command} is disabled.\n")
            return False

        with open(f'./HbaseCollections/{command}.csv', 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)
            print("{:<10} {:<30}".format("ROW", "COLUMN+CELL"))
            for row in reader:
                if len(headers) != len(row):
                    print('\n')
                    return False
                row_key = row[0]
                row_str = "{:<10} ".format(row_key)
                column_cell_str = ""
                for i in range(1, len(headers)):
                    column_cell_str += "{}: ={}, ".format(headers[i], row[i])
                column_cell_str = column_cell_str.rstrip(', ')
                print(row_str + column_cell_str)

        return True

    def delete(self, command: str) -> bool:
        command = command.replace("delete", "")
        commands = command.split(",")
        if len(commands) == 3:
            table_name = commands[0].replace(" ", "").replace("'", "")
            column_name = commands[1].replace(" ", "").replace("'", "")
            timestamp = commands[2].replace(" ", "").replace("'", "")
            found = False
            with open(f'./HbaseCollections/{table_name}.csv', 'r') as file:
                reader = csv.reader(file)
                rows = list(reader)
                for row in rows:
                    if len(row) > 0 and row[0] == column_name:
                        found = True
                        row[-1] = timestamp
                        break
                if not found:
                    print(
                        f"\n=> Hbase::delete - Row ({column_name}) not found in table {table_name}\n")

            with open(f'./HbaseCollections/{table_name}.csv', 'w+') as file:
                writer = csv.writer(file)
                writer.writerows(rows)
            print(
                f"\n=> Hbase::delete - ({column_name}) deleted in {table_name} at {timestamp}\n")
            return True

        else:
            print(f"\n=> Hbase::delete - Incorrect command format {command}\n")

        return False

    def count(self, table_name: str, search_param: str = None) -> int:
        # Verificar si la tabla existe
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False

        with open(f'./HbaseCollections/{table_name}.csv', 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)

            # Contar el total de filas
            if search_param is None:
                count = 0
                for row in reader:
                    count += 1
                print(f"\n=> Hbase::Table - {table_name} has {count} rows.\n")
                return count

            # Contar solo filas que coinciden con el parÃÂ¡metro de bÃÂºsqueda
            else:
                row_count = 0
                for row in reader:
                    if search_param in row:
                        row_count += 1
                print(
                    f"\n=> Hbase::Table - {table_name} has {row_count} rows that match the search parameter '{search_param}'.\n")
                return row_count

    def truncate(self, table_name: str) -> bool:
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False

        if self.check_string_in_file(table_name):
            print(f"\n=> Hbase::Table - {table_name} is disabled.\n")
            return False

        self.disable(f"disable '{table_name}'")

        # abrir el archivo de la tabla
        with open(f'./HbaseCollections/{table_name}.csv', 'r') as file:
            writer = csv.writer(file)
            headers = next(writer)
            writer.writerow(headers)
        print(f"\n=> Hbase::Table - {table_name} truncated.\n")

        self.disable(f"enable '{table_name}'")
        return True
    
    def enable(self, command):
        command = command.replace("enable", "").replace(' ', '').split(",")
        if len(command) < 1:
            print(
                f"\nValue error on: {command}\nToo many arguments for enable fuction.\nUsage: enable '<table_name>'\n"
            )
            return False
        command = command[0].replace("'", "")
        
        direc = './HbaseCollections'
        filename = f'{command}.csv'
        filepath = os.path.join(direc, filename)
        
        if os.path.exists(filepath):
            with open("./disabled_tables.txt", "r") as file:
                lines = file.readlines()
            with open("./disabled_tables.txt", "w") as file:
                for line in lines:
                    if line.strip("\n") != command:
                        file.write(line)
        else:
            print(f"\n=> Hbase::Table - {command} does not exist.\n")
            return False    

    def disable(self, command):
        # Setting the start time of the function
        start = time.time()
        # Removing the disable command from the command and splitting the command
        command = command.replace("disable", "").replace(' ', '').split(",")
        # Checking the command syntax
        if len(command) < 1:
            print(
                f"\nValue error on: {command}\nToo many arguments for disable fuction.\nUsage: disable '<table_name>'\n"
            )
            return False
        elif command[0][-1] != "'" or command[0][0] != "'":
            print(
                f"\nSyntax error on: {command[0]}\nCorrect use of single quotes is required.\nUsage: disable '<table_name>'\n"
            )
            return False
        # Getting the table name from the command
        command = command[0].replace("'", "")

        direc = './HbaseCollections'
        filename = f'{command}.csv'
        file_path = os.path.join(direc, filename)

        # Checking if the table exists
        if os.path.exists(file_path):
            # Write the name of the table on the disabled tables txt file
            with open("./disabledTables.txt", 'a+') as file:
                file.seek(0)
                file_contents = file.read()
                if command not in file_contents:
                    file.write(command)
                    file.write('\n')
        else:
            print(f"\n=> Hbase::Table - {command} does not exist.\n")
            return False

        # Setting the end time of the function and printing the results
        end = time.time()
        # printing the results
        print(f'0 row(s) in {round(end-start,4)} seconds')
        print(f"\n=> Hbase::Table - {command} disabled")

        return True
    
    

    # Modifies a table
    def alter(self, command: str) -> bool:
        # Setting the start time of the function
        start_time = time.time()
        # Removing the alter command from the command and splitting the command
        command = command.replace("alter", "").replace(' ', '').split(",")
        if len(command) < 3:
            print(
                f"\nValue error on: {command}\nToo few arguments for alter fuction.\nUsage: alter '<table_name>', '<column_family_name>', '<column_family_action>'\n"
            )
            return False

        # Getting the meta from the command
        value = command[0].replace("'", "")
        cf = command[1].replace("'", "")
        action = command[2].replace("'", "")

        # Checking if the table exists
        if not os.path.exists(f"./HbaseCollections/{value}.csv"):
            print(f"\n=> Hbase::Table - {value} does not exist.\n")
            return False

        # Updating the headers of the table
        df = pd.read_csv(f"./HbaseCollections/{value}.csv")
        if action == "add":
            if cf not in df.columns:
                df[cf] = ""
                df.to_csv(f"./HbaseCollections/{value}.csv", index=False)
                print(f"\n=> Hbase::Table - Added {cf} to {value}.\n")
                return True
            else:
                print(f"\n=> Hbase::Table - {cf} already exists in {value}.\n")
                return False
        elif action == "delete":
            if cf in df.columns:
                df = df.drop(columns=[cf])
                # df.drop(columns=[cf], inplace=True)
                df.to_csv(f"./HbaseCollections/{value}.csv", index=False)
                print(f"\n=> Hbase::Table - Deleted {cf} from {value}.\n")
                return True
            else:
                print(f"\n=> Hbase::Table - {cf} does not exist in {value}.\n")
                return False
        else:
            print(
                f"\nValue error on: {command}\nUnknown command for alter fuction.\nUsage: alter '<table_name>', '<column_family_name>', 'add/delete'\n"
            )
            return False



    def drop(self, table_name: str) -> bool:

        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False
        os.remove(f'./HbaseCollections/{table_name}.csv')
        self.table_names.remove(table_name)
        print(f"\n=> Hbase::Table - {table_name} dropped.\n")
        return True

    def dropAll(self):
        for table_name in self.table_names:
            self.drop(table_name)
            os.remove(f'./HbaseCollections/{table_name}.csv')
            self.table_names = []
            print("\n=> Hbase::All tables dropped\n")
            return True

    # Describes a table
    def describe(self, command: str) -> bool:
        # Setting the start time of the function
        start_time = time.time()
        # Removing the describe command from the command and splitting the command
        command = command.replace("describe", "").replace(' ', '').replace("'", "")
        # Checking if the table exists
        if not os.path.exists(f"./HbaseCollections/{command}.csv"):
            print(f"\n=> Hbase::Table - {command} does not exist.\n")
            return False

        # Getting the headers of the table
        df = pd.read_csv(f"./HbaseCollections/{command}.csv")
        headers = df.columns.tolist()
        # Printing the results
        print(f"\nTable {command}")
        print(f"{len(headers)} column(s)")
        for header in headers:
            print(header)
        # Setting the end time of the function
        end_time = time.time()
        print(f"\n=> Hbase::Table - {command} described in {round(end_time - start_time, 4)} seconds\n")
        return True

    # Creates a table
    def create(self, command: str) -> bool:
        # Setting the start time of the function
        start_time = time.time()
        # Removing the create command from the command and splitting the command
        command = command.replace("create", "").replace(' ', '').split(",")
        if len(command) < 2:
            print(
                f"\nValue error on: {command}\nToo few arguments for create fuction.\nUsage: create '<table_name>', '<column_family_name>'\n"
            )
            return False
        # Getting the meta from the command
        value = command[0].replace("'", "")
        timestamp = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        rowKey = len(self.table_names) + 1

        # checking if the table already exists
        if value in self.table_names:
            print(f"\n=> Hbase::Table - {value} already exists.\n")
            return False

        # Checking the command syntax
        for spec in command:
            if spec[0] != "'" or spec[-1] != "'":
                print(
                    f"\nSyntax error on: {spec}\nCorrect use of single quotes is required.\nUsage: create '<table_name>', '<column_family_name>'\n"
                )
                return False

        command = [spec.replace("'", "") for spec in command]
        command.insert(0, 'id')

        # setting the meta data
        meta_data = {}
        meta_data['Row Key'] = rowKey
        meta_data['Timestamp'] = timestamp
        meta_data['Value'] = value

        # removing the table name from the list
        command.remove(value)

        # Adding the meta data to the default table
        # If the table already exists, it will be updated, else it will be created
        if not os.listdir("./HbaseCollections"):
            filename = os.path.join("./HbaseCollections", f"TABLE.csv")
            df = pd.DataFrame(meta_data, index=[0])
            df.to_csv(filename, index=False)
        else:
            filename = os.path.join('./HbaseCollections', 'TABLE.csv')
            df = pd.DataFrame(meta_data, index=[0])
            df.to_csv(filename, mode='a', header=False, index=False)

        # Adding the headers to de HFile
        with open(f"./HbaseCollections/{value}.csv", 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(command)

        # Adding the table to the tables dictionary
        self.table_names.append(value)
        # Setting the end time of the function and printing the results
        end_time = time.time()
        print(f'0 row(s) in {round(end_time - start_time,4)} seconds')
        print(f"\n=> Hbase::Table - {value} created")
        return True

    # Lists all the tables
    def list_(self):

        files = os.listdir("./HbaseCollections")
        for file in files:
            if os.path.isfile(os.path.join("./HbaseCollections", file)):
                print(file.replace(".csv", ""))

    def delete_all(self, command: str) -> bool:
        table_name = command.split(" ")[1].replace("'", "")
        if table_name not in self.table_names:
            print(f"\n=> Hbase::Table - {table_name} does not exist.\n")
            return False
        # Deshabilitar la tabla
        self.disable(f"disable '{table_name}'")
        total_rows = self.count_rows(table_name)
        # Borrar el archivo .csv de la tabla
        os.remove(f"./HbaseCollections/{table_name}.csv")
        # Volver a habilitar la tabla
        self.disable(f"enable '{table_name}'")
        # Imprimir filas eliminadas
        print(f"\n=> Hbase::Table - {table_name} deleted {total_rows} rows.\n")
        return True

    def mainHBase(self):
        is_enabled = True
        counter = 0
        initial = input("\n[cloudera@quickstart ~]$ ")
        # time.sleep(2)
        # Start the Hbase shell
        if initial == "hbase shell":
            print(
                f"{datetime.today().strftime('%Y-%m-%d')} \nINFO [main] Configuration.deprecation: hadoop.native.lib is deprecated. Instead, use io.native.lib.available\n Hbase shell enter 'help<RETURN>' for list of supported commands. Type 'exit<RETURN>' to leave the HBase Shell\n Version 1.4.13, rUnknown\n")

            command = ""
            while is_enabled and command != "exit<RETURN>" or command != "exit":
                # User enters any command of the Hbase shell
                command = input(f"hbase(main):00{counter}:0>")
                counter += 1

                # Status commmand
                if command == 'status':
                    print(
                        '1 active master, 0 backup masters, 1 servers, 0 dead, 1.0000 average load')

                # Version command
                elif command == 'version':
                    print(f'1.4.13, rUnknown, {datetime.today().strftime("%Y-%m-%d")}')

                # TODO table help command
                elif command == 'table_help':
                    pass
                # whoami command
                elif command == "whoami":
                    print("cloudera (auth:SIMPLE)\n     groups: cloudera, default")

                # Create table command
                elif 'create' == command.split(" ")[0]:
                    # TODO Implement create table function
                    self.create(command)

                # List tables command
                elif command == 'list':
                    self.list_()

                elif 'alter' == command.split(" ")[0]:
                    self.alter(command)

                elif 'describe' == command.split(" ")[0]:
                    self.describe(command)

                # Disable table command
                elif 'disable' == command.split(" ")[0]:
                    self.disable(command)
                    
                elif 'enable' == command.split(" ")[0]:
                    self.enable(command)

                elif 'scan' == command.split(" ")[0]:
                    self.scan(command)

                elif 'count' == command.split(" ")[0]:
                    # conseguir el nombre de la tabla y el nombre de la columna
                    args = command.split(" ")
                    if len(args) < 2 or len(args) > 3:
                        print(
                            "Usage: count '<table_name>' [, '<search_string>']")
                    elif len(args) == 2:
                        table_name = args[1].replace("'", "")
                        self.count(table_name)
                    else:
                        table_name = args[1].replace("'", "")
                        search_string = args[2].replace("'", "")
                        self.count(table_name, search_string)

                # Drop table command
                elif 'drop' == command.split(" ")[0]:
                    table_name = command.split(" ")[1].replace("'", "")
                    hbase.drop(table_name)

                elif 'drop_all' == command.split(" ")[0]:
                    hbase.drop_all()

                elif 'delete' == command.split(" ")[0]:
                    self.delete(command)

                elif 'deleteall' == command.split(" ")[0]:
                    self.delete_all(command)

                elif 'truncate' == command.split(" ")[0]:
                    table_name = command.split(" ")[1].replace("'", "")
                    self.truncate(table_name)

                elif 'put' == command.split(" ")[0]:
                    self.put(command)

                elif 'get' == command.split(" ")[0]:
                    self.get(command)

                elif command != '':
                    print(f"ERROR: Unknown command '{command}'")
        elif initial != '':
            print(f"ERROR: Unknown command '{initial}'")

    def load_table(self, table: str):
        if table not in self.tables:
            if not self.table_exists(table):
                print(f"\n=> Hbase::Table - {table} does not exist.\n")
                return False
            else:
                self.tables[table] = pd.read_csv(f"./HbaseCollections/{table}.csv")
        else:
            self.tables[table] = pd.read_csv(f"./HbaseCollections/{table}.csv")  # Add this line

    
    def put(self, command: str) -> bool:
        command = command.replace("put", "").replace(' ', '').split(",")
        command = [spec.replace("'", "") for spec in command]

        # Extract the table name and check if it exists
        table = command.pop(0)
        self.load_table(table)

        if table not in self.tables:
            return False

        df = self.tables[table]

        id = command[0]
        column_subcol = command[1].split(":")
        value = command[2]

        if len(column_subcol) == 2:
            column = column_subcol[0]
            subcol = column_subcol[1]
        else:
            print("SyntaxError: invalid syntax on", command[1])
            return False

        if id not in df['id'].values:
            # Create a new row with the given id
            new_row = {col: '' for col in df.columns}
            new_row['id'] = id
            new_row[column] = str({subcol: value})

            # Append the new row to the DataFrame
            df = df.append(new_row, ignore_index=True)
        else:
            # Update the existing row
            row_index = df[df['id'] == id].index[0]
            cell = df.loc[row_index, column]

            if pd.isna(cell) or cell == '':
                # Create a new dictionary if the cell is empty
                df.loc[row_index, column] = str({subcol: value})
            else:
                # Update the dictionary if the cell already contains data
                cell_dict = ast.literal_eval(cell)
                cell_dict[subcol] = value
                df.loc[row_index, column] = str(cell_dict)

        # Save the updated DataFrame to the CSV file
        df.to_csv(f"./HbaseCollections/{table}.csv", index=False)

        return True


def clear_screen():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')


hbase = HbaseSimulator()
clear_screen()
# hbase.mainHBase()

#                tabla       id    columna:subcolumna        valor
hbase.put("put 'empleado', '100', 'personal_data:fullname', 'Jorge Caballeros'")


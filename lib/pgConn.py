import psycopg2
import io 
import subprocess
from datetime import datetime

class dbConn():
    def __init__(self, host='127.0.0.1', port=5432, dbname='postgres', user='postgres', password=''):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user 
        self.password = password
        self.now = ''
        try :
            print('Connection')
            self.conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password)
            self.cur = self.conn.cursor()
        except Exception as e:
            print(f'Connection error : {e}')
            return None
        
    def get_now(self) :
        self.now = datetime.now().strftime("%Y%m%d%H%M%S_%f")
        return self.now
    
    def dml_execute(self, query) :
        try :
            self.cur.execute(query)
            self.conn.commit()
        except Exception as e :
            print(f'DML Execute Error : {e}')
    def select_execute(self, query) :
        try :
            self.cur.execute(query)
            return self.cur.fetchall()
        except Exception as e :
            print(f'Select Exception {e}')
    
    def ddl_index(self, query) :
        try :
            self.cur.execute(query)
        except Exception as e : 
            print(f'index fail: {e}\nQuery info : {query}')

    """def export (self, table_name):
        buffer = io.StringIO()
        self.cur.copy_expert(f'copy (select * from {table_name} limit 0) to STDOUT WITH CSV HEADER', buffer)
        buffer.seek(0)
        temp_import_table_name = f'{table_name}_DBA_{datetime.now().strftime("%Y%m%d%H%M%S")}'
        self.cur.copy_expert(f'copy {temp_import_table_name} from STDIN WITH CSV HEADER', buffer)
        self.conn.commit()"""
    
    def export (self, database='', table_name=[], dest_file='/backup',export_type='') :
        print(f'export files\ndatabase :{database}\ntable_name : {table_name}\ndest_file:{dest_file}')
        if database :
            if len(table_name):
                dest_file_name = f'{self.get_now()}_{self.host}_{database}_tables_backup.sql'
            else :
                # backup database
                dest_file_name = f'{self.get_now()}_{self.host}_{database}_backup.sql'
        else : 
            dest_file_name = f'{self.get_now()}_{self.host}_backup.sql'
        dest_file=f'{dest_file}/{dest_file_name}'
        print (f'export dest file : {dest_file}')        
        try:
            #pg_dump --host=127.0.0.1 --port=5432 --username=postgres --password='' > backup.sql
            #pg_dump --host=127.0.0.1 --port=5432 --username=postgres --no-password > backup.sql
            #pg_dump --host=127.0.0.1 --port=5432 --username=postgres --no-password --dbname=postgres --table=test_1 --table=test_new > test.sql
            #pg_dump --dbname=postgresql://postgres:@127.0.0.1:5432/postgres?table=test_1 > test.sql
            #print(f'export_info : {export_info}')
            """process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, export_info),
                 '-Fc',
                 '-f', dest_file,
                 '-v'],
                stdout=subprocess.PIPE
            )"""
            command =['pg_dump',
                    '--host={}'.format(self.host),
                    '--port={}'.format(self.port),
                    '--username={}'.format(self.user),
                    '-Fc',
                    '-f', dest_file,
                    '-v']
            if self.password :
                command.extend(['--password={}'.format(self.password)])
            else :
                command.extend(['--no-password'])

            if len(table_name) :
                for table in table_name:
                    command.extend(["--table", table])

            if export_type == 'schema-only' : 
                #schema only
                command.extend(["--schema-only"])
            elif export_type == 'data-only' :
                #data only 
                command.extend(["--data-only"])
            else : 
                print('Full dump')

            print(f'command : {command}')
            process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)

    def pg_import(self,import_fullpath,database='postgres'):
        print('import')
        command =['pg_restore',
                    '--host={}'.format(self.host),
                    '--port={}'.format(self.port),
                    '--username={}'.format(self.user),
                    '-d={}'.format(database),
                    '-Fc', import_fullpath]
        try :
            if self.password :
                command.extend(['--password={}'.format(self.password)])
            else :
                command.extend(['--no-password'])
            process = subprocess.Popen(
                        command,
                        stdout=subprocess.PIPE
                )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)
        
    def close(self):
        """_summary_
            close
        """
        self.cur.close()
        self.conn.close()

if __name__ == "__main__":
    # dump_test = export : 0 / import : 1
    dump_test = 1
    test_db = dbConn(host='127.0.0.1', port=5432, dbname='postgres', user='postgres', password='')
    query = """
        select * from test_1
    """
    result = test_db.select_execute(query)
    print(f'result : {result}')
    if not dump_test :
        test_db.export(dest_file='/home')
        print('======================================================')
        test_db.export(database='postgres',dest_file='/home')
        print('======================================================')
        test_db.export(database='postgres', table_name=['test_new'],dest_file='/home')
        print('======================================================')
        test_db.export(database='postgres', table_name=['test_new','test_1'],dest_file='/home')
    else :
        test_db.pg_import(import_fullpath='/home/20240824020728_416001_127.0.0.1_postgres_tables_backup.sql')

    chk_host='127.0.0.1'
    chk_port=5432
    chk_dbname='postgres'
    chk_user='postgres'
    chk_password=''
    
    test_db.close()

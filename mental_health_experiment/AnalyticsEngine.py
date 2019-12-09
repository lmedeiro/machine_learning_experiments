import sqlalchemy as sql
import pandas as pd
import pytz
import numpy as np
import time


TZ = pytz.timezone('Europe/Helsinki')

class DataLoadingSystem(object):

    def __init__(self, db_owner, sql_pass, db_name,
                 dbms_name='postgresql', host_address='localhost', port_number='5432'):
        """
        The DataLoadingSystem loads information from specific DMBS to dataframes.

        :param db_owner: str
        :param sql_pass: str
        :param db_name: str
        :param dbms_name: str
        :param host_adress: str
        """
        self.engine = sql.create_engine(dbms_name + '://' + db_owner + ':' + sql_pass + '@' + host_address + ':' + port_number + '/' + db_name)
        #print("DataLoadingSystem called")

    def get_data_table(self, table_name, index_col='date'):
        conn = self.engine.connect()
        result_table = pd.read_sql_table(table_name, conn, index_col=index_col)
        result_table.index = result_table.index.tz_convert(TZ)
        return result_table

    def get_data_for_specific_date_range(self, table_name, start_date, end_date, index_col='timestamp'):
        """
        Function returns pandas dataframe with data specific for the dates provided.

        :param table_name: str
        :param start_date: str
        :param end_date: str
        :param index_col: str
        :return:  pandas dataframe
        """
        query = """select * from "{}" where ("timestamp" >= '{}')  ;""".format(table_name,
                                                                               start_date)
        conn = self.engine.connect()
        result_query = pd.read_sql_query(query, conn,
                                         index_col=index_col)
        result_query.index = pd.to_datetime(result_query.index,
                                            utc=True)
        result_query.index = result_query.index.tz_convert(TZ)
        #print(type(result_query.index))
        return result_query


if __name__ == "__main__":
    sql_pass = "c0c029ac4b9cb53ec071dd8dc2f6ab626efee799e8796e3476e1cacb8c30eee6"
    # postgres://dcxcpcgrfhaiyl:c0c029ac4b9cb53ec071dd8dc2f6ab626efee799e8796e3476e1cacb8c30eee6@ec2-54-75-245-196.eu-west-1.compute.amazonaws.com:5432/dcpbo9e1rj0sna
    host_address = "ec2-54-75-245-196.eu-west-1.compute.amazonaws.com"
    db_name = 'dcpbo9e1rj0sna'
    owner = 'dcxcpcgrfhaiyl'
    port_number = '5432'

    data_loader = DataLoadingSystem(db_owner=owner,
                                    db_name=db_name,
                                    sql_pass=sql_pass,
                                    host_address=host_address,
                                    port_number=port_number)
    # data_loader.engine.
    end_date = time.strftime('%Y-%m-%d')
    start_date = "2015-12-28"
    # end_date = '2019-01-01'
    table_name = 'mood'
    data = data_loader.get_data_for_specific_date_range(table_name=table_name, start_date=start_date,
                                                        end_date=end_date)
    print(data)
    print('DONE')

from lstore.table import *
from lstore.index import Index
from time import process_time

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.currentRID = table.total_records
        self.index = Index(self.table)
        self.has_index = False
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        if(self.has_index == False):                                # ----------------------------
            self.index.create_index(self.table, self.table.key)     # Remove the Corresponding RID
            self.has_index = True                                   # from Page_Directory and
        rid = self.index.remove(key)                                # Index Tree  
        if(rid == None):                                            # 
            print("Key Not Found")                                  #
            return None                                             # *Actual Data is Not Deleted
        del self.table.page_directory[rid]                          # ----------------------------
        self.table.total_records = self.table.total_records - 1
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        length = len(columns)
        if(length > 0):
            if(self.table.page_full):  
    #---------- Create New pages ------------------#     
                new_page_index = self.table.num_base_page
                num_column = self.table.num_columns+4
                basefilename = self.table.disk_directory + "/b_" + str(new_page_index)
                tailfilename = self.table.disk_directory + "/t_" + str(new_page_index)
                for i in range(num_column):
                    file = open(basefilename + "c_"+str(i), "w+")
                    file.close()                       
                    file = open(tailfilename + "c_"+str(i), "w+")   
                    file.close()          
                new_base = [Page(basefilename + "c_"+str(i), (self.table.name, "b", new_page_index, i)) for i in range(num_column)]
                new_tail = [Page(tailfilename + "c_"+str(i), (self.table.name, "t", new_page_index, i)) for i in range(num_column)]                               
                self.table.num_base_page += 1            
                self.table.num_tail_page += 1             
                self.table.page_full = False 
     #-----------Assign new page to Bufferpool--------------------#   
                for i in range(len(new_base)):
                    index = self.table.bufferpool.getindex(self.table.name, "b", new_page_index, i)
                    self.table.bufferpool.write(index,page=new_base[i])
                for i in range(len(new_tail)):
                    index = self.table.bufferpool.getindex(self.table.name, "t", new_page_index, i)
                    self.table.bufferpool.write(index,page=new_tail[i])
    # ------ Write Meta-data to pages in Bytes ------ #
            base_pages_index = self.table.num_base_page - 1
            schema_encoding = '0' * length
            indirection_to_bytes = (0).to_bytes(8, 'big')
            schema_to_bytes = bytearray(8)
            schema_to_bytes[0:4] = bytearray(schema_encoding, 'utf-8')
            rid_to_bytes = self.currentRID.to_bytes(8, 'big')
            time_to_bytes = int(process_time()).to_bytes(8, 'big')
            index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, INDIRECTION_COLUMN)
            self.table.bufferpool.write(index,value=indirection_to_bytes)
            index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, RID_COLUMN)
            self.table.bufferpool.write(index,value=rid_to_bytes)
            index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, TIMESTAMP_COLUMN)
            self.table.bufferpool.write(index,value=time_to_bytes)
            index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, SCHEMA_ENCODING_COLUMN)
            self.table.bufferpool.write(index,value=schema_to_bytes)
    # ------ End Writing Meta-Data ------ #
    # ------ Write Actual Data to Pages ------ #
            for i in range(self.table.key, length+4):
                value_to_bytes = columns[i-4].to_bytes(8, 'big')
                index = index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, i)
                self.table.bufferpool.write(index,value=value_to_bytes)
    # ------ Done ------ #
            slot = self.table.bufferpool.get(index).num_records - 1
            self.table.page_directory[self.currentRID] = (base_pages_index, slot)   # Add to Page_Directory
            self.currentRID += 1
            self.table.total_records += 1
            if not(self.table.bufferpool.get(index).has_capacity()):
                self.table.page_full = True
    
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, column, query_columns):
        list = []
        new_column = []
        column = column + 4
        if(self.has_index == False):                                # -----------------------------------------
            self.index.create_index(self.table, column)     # Create an Index Tree if there is not one
            self.has_index = True                                   # -----------------------------------------
        rid = self.index.locate(key)                                # Find RID using Index Tree
        if(rid == None):                                            #
            print("Key Not Found\n")                                #
            return None                                             # ------------------------------------------
        page_index, slot = self.table.page_directory[rid]           # Use RID to Locate Actual Data
    # ------ Read the Origin Data ------ #
        for i in range(0, len(query_columns)):
            if(query_columns[i] == 1):
                colunm_value_bytes = self.table.base_records[page_index][i+4].read(slot)
                new_column.append(int.from_bytes(colunm_value_bytes, 'big'))
            else:
                new_column.append(None)

    # ------ Check Schema Code for Updated Data ------ #
        schema_bytes = self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].read(slot)
        schema = schema_bytes[0:5].decode('utf-8')
        for i in range(0, self.table.num_columns):
            # --- Replace Origin Data with Updated Data --- #
            if(schema[i] == '1' and query_columns[i] == 1):
                indirection_bytes = self.table.base_records[page_index][INDIRECTION_COLUMN].read(slot)
                indirection = int.from_bytes(indirection_bytes, 'big')
                updated_value_bytes = self.table.tail_records[page_index][i+4].read(indirection)
                updated_value = int.from_bytes(updated_value_bytes, 'big')
                new_column[i] = updated_value
    # ------ Done ------ #
        record = Record(rid, key, new_column)
        list.append(record)
        return list

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        if(self.has_index == False):                                # -------------------------
            self.index.create_index(self.table, self.table.key)     # Check for Index Creation
            self.has_index = True                                   # -------------------------
        rid = self.index.locate(key)
        if(rid == None):
            print("Key Not Found")
            return None
        page_index, slot = self.table.page_directory[rid]
    # ------ Read Schema Code for Checking Updated Data ------ #
        new_schema_to_bytes = self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].read(slot)
        tail_indirection_to_bytes = self.table.base_records[page_index][INDIRECTION_COLUMN].read(slot)
        tail_indirection = int.from_bytes(tail_indirection_to_bytes, 'big')
        for i in range(0, len(columns)):
            value_to_bytes = bytearray(8)
            # --- Read an Updated Data if exists --- #
            if(new_schema_to_bytes[i] == ord('1')):
                value_to_bytes = self.table.tail_records[page_index][i+4].read(tail_indirection)
            # --- Write the new Updating Data to tail --- #
            if(columns[i] != None):
                new_schema_to_bytes[i] = ord('1')
                value_to_bytes = columns[i].to_bytes(8, 'big')
            self.table.tail_records[page_index][i+4].write(value_to_bytes)
    # ------ Write Indirection for Base and Tail Indirection Page ------ #
            new_tail_indirection = self.table.tail_records[page_index][i+4].num_records - 1
        tail_indirection_to_bytes = new_tail_indirection.to_bytes(8,'big')
        self.table.base_records[page_index][INDIRECTION_COLUMN].change_value(slot, tail_indirection_to_bytes)
        if(tail_indirection == 0):
            slot_to_bytes = slot.to_bytes(8,'big')
            self.table.tail_records[page_index][INDIRECTION_COLUMN].write(slot_to_bytes)
        else:
            self.table.tail_records[page_index][INDIRECTION_COLUMN].write(tail_indirection_to_bytes)
    # ------ Write new Meta-Data for Tail Pages ------ #
        rid_to_bytes = rid.to_bytes(8,'big')
        time_to_bytes = int(process_time()).to_bytes(8,'big')
        self.table.tail_records[page_index][RID_COLUMN].write(rid_to_bytes)
        self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].change_value(slot, new_schema_to_bytes)
        self.table.tail_records[page_index][SCHEMA_ENCODING_COLUMN].write(new_schema_to_bytes)
        self.table.tail_records[page_index][TIMESTAMP_COLUMN].write(time_to_bytes)
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        result = 0
        if(self.has_index == False):                                # -----------------------------
            self.index.create_index(self.table, self.table.key)     # Check Index Creation
            self.has_index = True                                   # -----------------------------
        for key in range(start_range, end_range+1):
            rid = self.index.locate(key)
            if(rid == None):
                result += 0
                continue
        # ------ If an Key exists, Read the Corresponding Value ------ #
            page_index, slot = self.table.page_directory[rid]
            schema_to_bytes = self.table.base_records[page_index][SCHEMA_ENCODING_COLUMN].read(slot)
            schema = schema_to_bytes[0:5].decode('utf-8')
        # ------ Check for Updated Value ------ #
            if(schema[aggregate_column_index] == '1'):
                indirection_to_bytes = self.table.base_records[page_index][INDIRECTION_COLUMN].read(slot)
                indirection = int.from_bytes(indirection_to_bytes, 'big')
                value_to_bytes = self.table.tail_records[page_index][aggregate_column_index+4].read(indirection)
                value = int.from_bytes(value_to_bytes, 'big')
            else:
                value_to_bytes = self.table.base_records[page_index][aggregate_column_index+4].read(slot)
                value = int.from_bytes(value_to_bytes, 'big')
        # ------ Sum up ------ #
            result += value
        return result

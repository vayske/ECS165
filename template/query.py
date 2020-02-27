from lstore.table import *
from lstore.index import Index
from time import process_time

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.currentRID = table.total_base_records
        self.currentTail_ID = table.total_tail_records
        self.index = Index(table.num_columns)
        self.index.create_index(self.table)
        self.has_index = True
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        ridList = self.index.remove(self.table.key - NUM_META_COLUMN,key)                                # Index Tree
        if(len(ridList) == 0):                                            #
            print("Key Not Found")                                  #
            return None
        else:
            for rid in ridList:
                page_index, slot = self.table.page_directory[str(rid)]
                rid_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, RID_COLUMN)
                ind_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, INDIRECTION_COLUMN)
                indirection = int.from_bytes(self.table.bufferpool.get(page_index).read(slot), 'big')
                tail_number = int(indirection / 512)
                tail_slot = int(indirection % 512)
                invalid_rid_to_bytes = (-1).to_bytes(8,'big', signed=True)
                #invalidate 
                self.table.bufferpool.get(rid_index).change_value(slot,invalid_rid_to_bytes)
                self.table.bufferpool.get(ind_index).change_value(slot,invalid_rid_to_bytes)

                #get last update, remove each column value from index  
                for i in range(1,self.table.num_columns):
                    index = self.table.bufferpool.getindex(self.table.name, "t", page_index, tail_number, i+self.table.key)
                    value = int.from_bytes(self.table.bufferpool.get(index).read(tail_slot), 'big')
                    self.index.remove(i,value)
                while indirection > 0:  #loop through all updates for this tail record
                    rid_index = self.table.bufferpool.getindex(self.table.name,"t", page_index, tail_number, RID_COLUMN)
                    ind_index = self.table.bufferpool.getindex(self.table.name,"t", page_index, tail_number, INDIRECTION_COLUMN)
                    indirection = int.from_bytes(self.table.bufferpool.get(page_index).read(tail_slot), 'big')
                    self.table.bufferpool.get(rid_index).change_value(tail_slot,invalid_rid_to_bytes)
                    self.table.bufferpool.get(ind_index).change_value(tail_slot,invalid_rid_to_bytes)
                    tail_number = int(indirection / 512)
                    tail_slot = int(indirection % 512)
                #as we get through all updates, we will get to the first update which has indirection value 0
                if indirection == 0:
                    rid_index = self.table.bufferpool.getindex(self.table.name,"t", page_index, tail_number, RID_COLUMN)
                    ind_index = self.table.bufferpool.getindex(self.table.name,"t", page_index, tail_number, INDIRECTION_COLUMN)
                    self.table.bufferpool.get(rid_index).change_value(tail_slot,invalid_rid_to_bytes)
                    self.table.bufferpool.get(ind_index).change_value(tail_slot,invalid_rid_to_bytes)
                # remove this rid from page directory 
                del self.table.page_directory[str(rid)]
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
                num_column = self.table.num_columns+NUM_META_COLUMN
                basefilename = self.table.disk_directory + "/b_" + str(new_page_index)
                tailfilename = self.table.disk_directory + "/t_" + str(new_page_index)
                for i in range(num_column):
                    file = open(basefilename + "p_0c_"+str(i), "w+")
                    file.close()                       
                    file = open(tailfilename + "p_0c_"+str(i), "w+")   
                    file.close()          
                new_base = [Page(basefilename + "p_0c_"+str(i), (self.table.name, "b", new_page_index, 0, i)) for i in range(num_column)]
                new_tail = [Page(tailfilename + "p_0c_"+str(i), (self.table.name, "t", new_page_index, 0, i)) for i in range(num_column)]                               
                self.table.num_base_page += 1            
                self.table.num_tail_page += 1             
                self.table.page_full = False 
     #-----------Assign new page to Bufferpool--------------------#   
                for i in range(len(new_base)):
                    index = self.table.bufferpool.getindex(self.table.name, "b", new_page_index, 0, i)
                    self.table.bufferpool.write(index,page=new_base[i])
                for i in range(len(new_tail)):
                    index = self.table.bufferpool.getindex(self.table.name, "t", new_page_index, 0, i)
                    self.table.bufferpool.write(index,page=new_tail[i])
    # ------ Write Meta-data to pages in Bytes ------ #
            base_pages_index = self.table.num_base_page - 1
            schema_encoding = '0' * length
            indirection_to_bytes = (0).to_bytes(8, 'big')
            schema_to_bytes = bytearray(8)
            schema_to_bytes[0:4] = bytearray(schema_encoding, 'utf-8')
            rid_to_bytes = self.currentRID.to_bytes(8, 'big')
            time_to_bytes = int(process_time()).to_bytes(8, 'big')
            index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, 0, INDIRECTION_COLUMN)
            self.table.bufferpool.write(index,value=indirection_to_bytes)
            index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, 0, RID_COLUMN)
            self.table.bufferpool.write(index,value=rid_to_bytes)
            index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, 0, TIMESTAMP_COLUMN)
            self.table.bufferpool.write(index,value=time_to_bytes)
            index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, 0, SCHEMA_ENCODING_COLUMN)
            self.table.bufferpool.write(index,value=schema_to_bytes)
    # ------ End Writing Meta-Data ------ #
    # ------ Write Actual Data to Pages ------ #
            for i in range(self.table.key, length+NUM_META_COLUMN):
                value_to_bytes = columns[i-NUM_META_COLUMN].to_bytes(8, 'big')
                index = self.table.bufferpool.getindex(self.table.name, "b", base_pages_index, 0, i)
                self.table.bufferpool.write(index,value=value_to_bytes)
    # ------ Done ------ #
            slot = self.table.bufferpool.get(index).num_records - 1
            self.table.page_directory[str(self.currentRID)] = (base_pages_index, slot)   # Add to Page_Directory
            for i in range(0, self.table.num_columns):
                if(self.index.trees[i].has_key(columns[i])):
                    tempList = self.index.trees[i].get(columns[i])
                    tempList.append(self.currentRID)
                    self.index.trees[i].__setitem__(columns[i], tempList)
                else:
                    self.index.trees[i].insert(columns[i],[self.currentRID])

            self.currentRID += 1
            self.table.total_base_records += 1
            if not(self.table.bufferpool.get(index).has_capacity()):
                self.table.page_full = True
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, column, query_columns):
        list = []
        new_column = []
        ridList = self.index.locate(column,key)                                # Find RID using Index Tree
        if(len(ridList) == 0):                                            #
            print("Key Not Found\n")                                #
            return None
        for rid in ridList:
            #print("look for rid: " + str(rid) + " in page_directory")
            page_index, slot = self.table.page_directory[str(rid)]           # Use RID to Locate Actual Data
            #print("got result: page_index = " + str(page_index) + " slot = " + str(slot))
            # ------ Read the Origin Data ------ #
            for i in range(0, len(query_columns)):
                if(query_columns[i] == 1):
                    index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, i+NUM_META_COLUMN)
                    column_value_bytes = self.table.bufferpool.get(index).read(slot)
                    new_column.append(int.from_bytes(column_value_bytes, 'big'))
                else:
                    new_column.append(None)
            # ------ Check Schema Code for Updated Data ------ #
            sc_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, SCHEMA_ENCODING_COLUMN)
            schema_bytes = self.table.bufferpool.get(sc_index).read(slot)
            schema = schema_bytes[0:5].decode('utf-8')
            for i in range(0, self.table.num_columns):
                # --- Replace Origin Data with Updated Data --- #
                if(schema[i] == '1' and query_columns[i] == 1):
                    ind_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, INDIRECTION_COLUMN)
                    indirection_bytes = self.table.bufferpool.get(ind_index).read(slot)
                    indirection = int.from_bytes(indirection_bytes, 'big')
                    #calculate actual position for the tail record
                    tail_number = int(indirection / 512)
                    tail_slot = indirection % 512
                    tail_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, tail_number, i+NUM_META_COLUMN)
                    updated_value_bytes = self.table.bufferpool.get(tail_index).read(tail_slot)
                    updated_value = int.from_bytes(updated_value_bytes, 'big')
                    new_column[i] = updated_value
            # ------ Done ------ #
            self.table.bufferpool.pinned[page_index] = self.table.bufferpool.pinned[page_index] - 1
            record = Record(rid, key, new_column)
            list.append(record)
        return list

    """
    # Update a record with specified key and columns
    """
    def update(self, key, *columns):      
        if(key == 92106437):
            debug = 1
        else:
            debug = 0
        ridList = self.index.locate(self.table.key - NUM_META_COLUMN, key)
        rid = ridList[0]
        if(len(ridList) == 0):
            print("Key Not Found")
            return None
        page_index, slot = self.table.page_directory[str(rid)]
# ------ Read Schema Code for Checking Updated Data ------ #
        sc_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, SCHEMA_ENCODING_COLUMN)
        new_schema_to_bytes = self.table.bufferpool.get(sc_index).read(slot)
        base_ind_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, INDIRECTION_COLUMN)
        base_ind_bytes = self.table.bufferpool.get(base_ind_index).read(slot)
        base_ind = int.from_bytes(base_ind_bytes, 'big')
        # for example indirection = 512 -> tailnumber = 1, tailslot = 0
        tail_number = int(base_ind / 512)    #get the latest tail page number
        tail_slot = int(base_ind % 512)   #slot number of last update on this record 
        if debug:
            print(" ")
            lastest_data = self.select(key, 0, [1,1,1,1,1])[0]
            print('lastest_data for this base = ', lastest_data)
            print("old indirection value = " + str(base_ind) + " tail number for that = " + str(tail_number) + " tail slot for that = " + str(tail_slot))
            for j in range(5):
                index = self.table.bufferpool.getindex(self.table.name,"t",page_index,tail_number,j+self.table.key)
                value = int.from_bytes(self.table.bufferpool.get(index).read(tail_slot), 'big')
                print("tail record column[" + str(j) + "]: " + str(value))
        # get the latest tail page for this base page
        latest_tail_page_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, tail_number, self.table.key)

        #create new tail page if lastest tail page is full
        if self.table.bufferpool.get(latest_tail_page_index).num_records == 512:
            new_page_number = tail_number + 1
            num_column = self.table.num_columns + NUM_META_COLUMN
            newtailfilename = self.table.disk_directory + "/t_" + str(page_index) + "p_" + str(new_page_number)
            for i in range(num_column):
                file = open(newtailfilename + "c_" + str(i), "w+")
                file.close()
            new_tail = [Page(newtailfilename + "c_" + str(i),(self.table.name, "t", page_index, new_page_number, i)) for i in range(num_column)]
            self.table.num_tail_page += 1
            new_tail_number = new_page_number
            #Assign new page to Bufferpool
            for i in range(len(new_tail)):
                index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_page_number, i)
                self.table.bufferpool.write(index, page = new_tail[i])
        else: #if tail page still have space, we just insert into the current page
            new_tail_number = tail_number 
        
        #-----actual update-------------
        for i in range(0, len(columns)):
            value_to_bytes = bytearray(8)
        # --- Read an Updated Data if exists --- #
            if(new_schema_to_bytes[i] == ord('1')):         
                #if schema is 1, need to get latest value from last update 
                value_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, tail_number, i + self.table.key)
                value_to_bytes = self.table.bufferpool.get(value_index).read(tail_slot)
                if debug:
                    last_update = int.from_bytes(value_to_bytes,'big')
                    print("value in last update for column " + str(i) + " = " + str(last_update))
                    num_record = self.table.bufferpool.get(value_index).num_records
                    print("before update tail page has " + str(num_record) + " records")
            else: # schema is 0, just get the base value 
                value_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, i + self.table.key)
                value_to_bytes = self.table.bufferpool.get(value_index).read(slot)
                if debug:
                    last_update = int.from_bytes(value_to_bytes,'big')
                    print("value in base for column " + str(i) + " = " + str(last_update))
        # --- Write the new Updating Data to tail --- #
            if(columns[i] != None): #overwrite with latest value, or wirte the value from last update or base 
                new_schema_to_bytes[i] = ord('1')                   #assign new schema
                value_to_bytes = columns[i].to_bytes(8, 'big')
                if debug:
                    update = int.from_bytes(value_to_bytes,'big')
                    print("new value of column " + str(i) + " = " + str(update))
            #write to tail page, 
            value_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_tail_number, i+NUM_META_COLUMN)
            if debug:
                num_record = self.table.bufferpool.get(value_index).num_records
                print("before write tail page has " + str(num_record) + " records")
            self.table.bufferpool.write(value_index, value=value_to_bytes)
            #printing for test
            if debug:
                value_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_tail_number, i+NUM_META_COLUMN)
                new_value_slot = self.table.bufferpool.get(value_index).num_records - 1
                new_value = int.from_bytes(self.table.bufferpool.get(value_index).read(new_value_slot), 'big')
                num_record = self.table.bufferpool.get(value_index).num_records
                print("after updated value of column " + str(i) + " = " + str(new_value))
                print("after update tail page has " + str(num_record) + " records")
# ------ Write Indirection for Base Record ------ #
        index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_tail_number, self.table.key)
        new_tail_ind_slot = self.table.bufferpool.get(index).num_records -1                         #slot for the latest update
        #new indirection value to put onto the base record
        #  = number of total tail records for this base page.  --for example, update onto tailpage number 1 slot 0 -> 1*512 + 0 = 512 
        if new_tail_number > 0:
            new_base_ind = int(new_tail_number * 512 + new_tail_ind_slot)     #######
        else:
            new_base_ind = int(new_tail_ind_slot)
        new_base_ind_to_bytes = new_base_ind.to_bytes(8,'big') 
        if debug:
            print("new update is put in slot " + str(new_value_slot) + " in tailpage number " + str(new_tail_number))
            print("new indirection in base page = " + str(new_base_ind))
            for j in range(5):
                index = self.table.bufferpool.getindex(self.table.name,"t",page_index,new_tail_number,j+self.table.key)
                value = int.from_bytes(self.table.bufferpool.get(index).read(new_tail_ind_slot), 'big')
                print("after update tail record column[" + str(j) + "]: " + str(value))
        # get indirection column in base page 
        base_ind_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, INDIRECTION_COLUMN)
        self.table.bufferpool.get(base_ind_index).change_value(slot, new_base_ind_to_bytes)             #update indirection column
#-----------Write indirection for new tail record----------
        if(base_ind == 0):      #  if this is the first tail record for this record (last indirection is 0)
            tail_ind_to_bytes = (0).to_bytes(8,'big')
            tail_ind_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_tail_number, INDIRECTION_COLUMN)
            self.table.bufferpool.write(tail_ind_index, value= tail_ind_to_bytes)
        else:       #not the first tail record for the base record, just write the last indirection value  
            tail_ind_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_tail_number, INDIRECTION_COLUMN)
            self.table.bufferpool.write(tail_ind_index, value = base_ind_bytes)
# ------ Write new Meta-Data for Tail Pages ------ #
        rid_to_bytes = rid.to_bytes(8,'big')
        time_to_bytes = int(process_time()).to_bytes(8,'big')
        rid_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_tail_number, RID_COLUMN)
        self.table.bufferpool.write(rid_index, value=rid_to_bytes)
        #change schma code for base record
        b_sc_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, SCHEMA_ENCODING_COLUMN)
        self.table.bufferpool.get(b_sc_index).change_value(slot, new_schema_to_bytes)
        t_sc_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_tail_number, SCHEMA_ENCODING_COLUMN)
        self.table.bufferpool.write(t_sc_index, value=new_schema_to_bytes)
        time_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, new_tail_number, TIMESTAMP_COLUMN)
        self.table.bufferpool.write(time_index, value=time_to_bytes)
        self.table.total_tail_records += 1
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        result = 0
        for key in range(start_range, end_range+1):
            ridList = self.index.locate(self.table.key - NUM_META_COLUMN, key)
            if(ridList == None or len(ridList) == 0):
                result += 0
                continue
            for rid in ridList:
        # ------ If an Key exists, Read the Corresponding Value ------ #
                page_index, slot = self.table.page_directory[str(rid)]
                sc_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, SCHEMA_ENCODING_COLUMN)
                schema_to_bytes = self.table.bufferpool.get(sc_index).read(slot)
                schema = schema_to_bytes[0:5].decode('utf-8')
        # ------ Check for Updated Value ------ #
                if(schema[aggregate_column_index] == '1'):
                    ind_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, INDIRECTION_COLUMN)
                    indirection_to_bytes = self.table.bufferpool.get(ind_index).read(slot)
                    indirection = int.from_bytes(indirection_to_bytes, 'big')
                    tail_number = int(indirection / 512)
                    tail_slot = int(indirection % 512)
                    tail_index = self.table.bufferpool.getindex(self.table.name, "t", page_index, tail_number, aggregate_column_index+NUM_META_COLUMN)
                    value_to_bytes = self.table.bufferpool.get(tail_index).read(tail_slot)
                    value = int.from_bytes(value_to_bytes, 'big')
                else:
                    base_index = self.table.bufferpool.getindex(self.table.name, "b", page_index, 0, aggregate_column_index+NUM_META_COLUMN)
                    value_to_bytes = self.table.bufferpool.get(base_index).read(slot)
                    value = int.from_bytes(value_to_bytes, 'big')
        # ------ Sum up ------ #
                result += value
        return result

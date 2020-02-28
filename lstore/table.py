from lstore.page import *
from time import time
import os, sys, json
import threading
import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
NUM_META_COLUMN = 4

MERGE_INTERVAL = 1  # time interval between each merge


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
    def __str__(self):
        return str(self.columns)

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, bufferpool, num_basepage, num_tailpage, total_base_records, total_tail_records):
        self.name = name
        self.disk_directory = os.getcwd() + "/" + self.name
        if not os.path.isdir(self.disk_directory):
            os.makedirs(self.disk_directory)
        self.key = key + 4
        self.num_columns = num_columns
        self.bufferpool = bufferpool
        self.num_base_page = num_basepage
        self.num_tail_page = num_tailpage
        self.total_base_records = total_base_records
        self.total_tail_records = total_tail_records
        if total_base_records % 512 == 0:
            self.page_full = True               
        else:
            self.page_full = False
        self.page_directory = {}            # A Python Dictionary in the format {RID:(Page_Index, Slot), ...}
        self.tail_directory = {}
        tail_dict = self.disk_directory + "/tail_dict.json"         #given base page index,  return latest tail number for that range 
        page_dict = self.disk_directory + "/page_dict.json"
        #load page_dict
        if os.path.isfile(page_dict) and os.stat(page_dict).st_size > 0:
            with open(page_dict, "r") as fp:
                self.page_directory = json.load(fp)
            fp.close()
            page_index, slot = self.page_directory[str(0)]
        else:
            file = open(page_dict, "w+")
            file.close()
        #load tail_dict
        if os.path.isfile(tail_dict) and os.stat(tail_dict).st_size > 0:
            with open(tail_dict, "r") as fp:
                self.tail_directory = json.load(fp)
            fp.close()
        else:
            file = open(tail_dict, "w+")
            file.close()
        #print(self.name, self.num_columns, self.key, self.num_base_page, self.num_tail_page, self.total_base_records, self.total_tail_records)
        #"""""   
        # create background thread for merge
        merge_thread = threading.Thread(target=self.__merge, args=())
        merge_thread.daemon = True
        merge_thread.start()
        #"""""

    def write_meta_to_disk(self):
        meta_dict = {}
        meta_dict.update({'key': self.key-4})
        meta_dict.update({'num_column': self.num_columns})
        meta_dict.update({'num_basepage': self.num_base_page})
        meta_dict.update({'num_tailpage': self.num_tail_page})
        meta_dict.update({'total_base_records': self.total_base_records})
        meta_dict.update({'total_tail_records': self.total_tail_records})
        with open(self.disk_directory + '/metadata.json',"w") as fp:
            json.dump(meta_dict, fp)
        fp.close()
        
        with open(self.disk_directory + '/page_dict.json',"w") as fp:
            json.dump(self.page_directory, fp)
        fp.close()

        with open(self.disk_directory + '/tail_dict.json', 'w') as fp:
            json.dump(self.tail_directory, fp)
        fp.close()



    def get_page_from_disk(self, filename, meta):
        if os.stat(filename).st_size > 0:
            txtfile = open(filename, "r")
            file_lines = txtfile.readlines()
            data_list = file_lines[1].split()
            txtfile.close()
            data = []
            for i in data_list:
                i_int = int(i)
                data.append(i_int)
            page = Page(filename, meta)
            page.lineage = int(file_lines[0])
            for i in range(len(data)):
                page.write(data[i].to_bytes(8, 'big'))
        else:
            page = Page(filename, meta)
        page.dirty = False
        return page

    def __merge(self):
        while True:
            time.sleep(MERGE_INTERVAL)
            print("start new merge")
            for page_index in range(self.num_base_page - 1):  # pick a base page
                # check num_of record for this base page by getting a whatever column of it 
                base_key_index = self.bufferpool.getindex(self.name, "b", page_index, 0, self.key)
                # if page is not full we don't merge this base page
                # since if someone insert during the merge, we don't have the latest base record in the merged copy 
                if self.bufferpool.get(base_key_index).has_capacity:
                    print("base page not full, don't merge")
                    continue  
                merge_number = 128                          # a nice number  512/128 = 4 merge to merge a whole tail page
                base_lineage = self.bufferpool.get(base_key_index).lineage  # check lineage of basepage
                tailpage_number = base_lineage / 512  # get corresponding tailpage that haven't merged
                tail_c_rid_index = self.bufferpool.getindex(self.name, "t", page_index, tailpage_number, RID_COLUMN)
                self.bufferpool.pinned[tail_c_0_index] = 1
                # check if tail has enough records to merge
                num_record_unmerged = self.bufferpool.get(tail_c_rid_index).num_record - base_lineage
                if num_record_unmerged < merge_number:
                    self.bufferpool.pinned[tail_c_0_index] = 0
                    print("not enough tail record to merge, stop merge")
                    continue
                base_copy = [Page("", ()) for i in range(self.num_columns)]  # empty pages
                for column in range(self.num_columns):  # for each column(of actual data )
                    # 这个p_0 是预定新加的 用来分辨tailpage的
                    copy_filename = self.disk_directory + "/b_" + str(page_index) + "p_0c_" + str(column)
                    # 这个0 和 p_0是一个东西
                    # page.meta = (table, "b"/"t",pageindex, pagenumber, column number)
                    page_copy = self.get_page_from_disk(copy_filename, (self.name, "b", page_index, 0, column))
                    num_record_infile = page_copy.num_record
                    if num_record_infile == 0:  # page haven't been written into file
                        page_in_pool_index = self.bufferpool.getindex(self.name, "t", page_index, 0, column)
                        self.bufferpool.pinned[page_in_pool_index] = 1
                        for i in range(self.bufferpool.get(page_in_pool_index).num_record):
                            page_data = self.bufferpool.get(page_in_pool_index).read(i)
                            page_copy.write(page_data, i)  # write page in pool to the copy
                        self.bufferpool.pinned[page_in_pool_index] = 1
                    page_copy.dirty = True
                    base_copy[column] = page_copy
                    print("copied base page")
                # get tail page from bufferpool and pin them
                tail_c_0_index = self.bufferpool.getindex(self.name, "t", page_index, tailpage_number, self.key)
                self.bufferpool.pinned[tail_c_0_index] = 1
                # use rid_column to find each corresponding base record of tail records
                tail_c_rid_index = self.bufferpool.getindex(self.name, "t", page_index, tailpage_number, RID_COLUMN)
                self.bufferpool.pinned[tail_c_rid_index] = 1
                # actual data column of tail records
                tail_c_1_index = self.bufferpool.getindex(self.name, "t", page_index, tailpage_number, self.key + 1)
                self.bufferpool.pinned[tail_c_1_index] = 1
                tail_c_2_index = self.bufferpool.getindex(self.name, "t", page_index, tailpage_number, self.key + 2)
                self.bufferpool.pinned[tail_c_2_index] = 1
                tail_c_3_index = self.bufferpool.getindex(self.name, "t", page_index, tailpage_number, self.key + 3)
                self.bufferpool.pinned[tail_c_3_index] = 1
                tail_c_4_index = self.bufferpool.getindex(self.name, "t", page_index, tailpage_number, self.key + 4)
                self.bufferpool.pinned[tail_c_4_index] = 1
                # merging starting from slot = lineage
                for slot in range(base_lineage, base_lineage + merge_number):
                    base_rid = self.bufferpool.get(tail_c_rid_index).read(slot)             #get rid for corresponding base record
                    base_index, base_slot = self.directory[str(base_rid)]                   #get slot from directory
                    print("start merging")
                    #read from tail, write to corresponding base records
                    data = self.bufferpool.get(tail_c_0_index).read(slot)
                    base_copy[0].write(base_slot, data)
                    data = self.bufferpool.get(tail_c_1_index).read(slot)
                    base_copy[1].write(base_slot, data)
                    data = self.bufferpool.get(tail_c_2_index).read(slot)
                    base_copy[2].write(base_slot, data)
                    data = self.bufferpool.get(tail_c_3_index).read(slot)
                    base_copy[3].write(base_slot, data)
                    data = self.bufferpool.get(tail_c_4_index).read(slot)
                    base_copy[4].write(base_slot, data)
                # end actual merging
                # update lineage
                base_copy[0].lineage = base_lineage + merge_number
                base_copy[1].lineage = base_lineage + merge_number
                base_copy[2].lineage = base_lineage + merge_number
                base_copy[3].lineage = base_lineage + merge_number
                base_copy[4].lineage = base_lineage + merge_number
                self.bufferpool.get(tail_c_0_index).lineage = base_lineage + merge_number
                self.bufferpool.get(tail_c_1_index).lineage = base_lineage + merge_number
                self.bufferpool.get(tail_c_2_index).lineage = base_lineage + merge_number
                self.bufferpool.get(tail_c_3_index).lineage = base_lineage + merge_number
                self.bufferpool.get(tail_c_4_index).lineage = base_lineage + merge_number
                # unpin tail pages
                self.bufferpool.pinned[tail_c_rid_index] = 0
                self.bufferpool.pinned[tail_c_0_index] = 0
                self.bufferpool.pinned[tail_c_1_index] = 0
                self.bufferpool.pinned[tail_c_2_index] = 0
                self.bufferpool.pinned[tail_c_3_index] = 0
                self.bufferpool.pinned[tail_c_4_index] = 0
                # change directory so that new query lead to this copy
                # get a unused bufferpool index
                for column in range(self.num_columns):
                    copy_index = self.bufferpool.empty.pop()
                    self.used.append(copy_index)
                    self.LRUIndex[copy_index] = len(self.used) - 1
                    self.bufferpool.write(copy_index, page=base_copy[column])
                    # change bufferpool directory,
                    # such that new query for this page get this new index
                    # instead of the index for the old base pages
                    self.bufferpool.directory.update({(self.name, "b", page_index, 0, column): copy_index})
                print("merging finish")
                

                #   each basepages  has 9 columns
                #   column      bufferpool_index    post_merge_bufferpool_index
                #   0           0                   0
                #   1           1                   1
                #   2           2                   2
                #   3           3                   3
                #   4           4                   9
                #   5           5                   10
                #   6           6                   11
                #   7           7                   12
                #   8           8                   13
                
                # after merge we always get the new indexs, 10,11,12,13 if we ask for these columns
                # and they are marked as dirty which will be written to disk

                # index for indirection column is never changed,
                # latest update always goes to this page even though it happens during the merge 
                # hence we still have the latest update 

                # 5,6,7,8 as the old index for old column, will be forgotten and eventually evicted
                # and since they are not dirty, they won't be written back to disk
                

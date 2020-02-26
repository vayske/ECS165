from lstore.page import *
from time import time
import os, sys, json
import threading
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
MERGE_INTERVAL = 3  # time interval between each merge


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

    def __init__(self, name, num_columns, key, bufferpool, num_basepage, num_tailpage, total_records):
        self.name = name
        self.disk_directory = os.getcwd() + "/" + self.name
        if not os.path.isdir(self.disk_directory):
            os.makedirs(self.disk_directory)
        self.key = key + 4
        self.num_columns = num_columns
        self.bufferpool = bufferpool
        self.num_base_page = num_basepage
        self.num_tail_page = num_tailpage
        self.total_records = total_records
        if total_records % 512 == 0:
            self.page_full = True
        else:
            self.page_full = False
        self.page_directory = {}  # A Python Dictionary in the format {RID:(Page_Index, Slot), ...}
        page_dict = self.disk_directory + "/page_dict.json"
        print("page directory json file has size: " + str(os.stat(page_dict).st_size))
        if os.stat(page_dict).st_size > 0:
            print("read page_directory json file")
            with open(page_dict, "r") as fp:
                self.page_directory = json.load(fp)
            fp.close()
            print("page_directory load from disk")
            page_index, slot = self.page_directory[str(0)]
            print("location for rid 0 = " + str(page_index) + " , " + str(slot))
        else:
            file = open(page_dict, "w+")
            file.close()

        # create background thread for merge
        merge_thread = threading.Thread(target=self.__merge, args=())
        merge_thread.daemon = True
        merge_thread.start()

    def write_meta_to_disk(self):
        meta_dict = {}
        meta_dict.update({'key': self.key - 4})
        meta_dict.update({'num_column': self.num_columns})
        meta_dict.update({'num_basepage': self.num_base_page})
        meta_dict.update({'num_tailpage': self.num_tail_page})
        meta_dict.update({'total_records': self.total_records})
        with open(self.disk_directory + '/metadata.json', "w") as fp:
            json.dump(meta_dict, fp)
        fp.close()

        with open(self.disk_directory + '/page_dict.json', "w") as fp:
            json.dump(self.page_directory, fp)
        fp.close()
        print("page_directory written to disk")
        page_index, slot = self.page_directory['0']
        print("location for rid 0 = " + str(page_index) + " , " + str(slot))

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
        # #merge_number = ?                   #number of tail page to be merge each time
        # """""
        # #如果你要系base page 搞， 就要每次都跑完整个base page 512 个record
        # #如果你不跑完全部base record, 就可能有些record indirectiion 在中间你没弄
        # #如果走tail page 假设你每次就弄100 个 那就只弄100个
        # #就拿tailpage， 假设弄100 个就每个tailpage 的data 覆盖回base page 的 record 上， 搞到100 个就停
        # #然后下次就从101 开始 再弄 100 个 这样。
        # #slot 0 到 100 这样 然后每个slot 你找到这个record的对应的base record 然后data改进去base 的record 里
        # #都要开新base 一样的 就是方向不同
        # #等于写进去10 次覆盖10次
        # 对 要选merge的base page 随机或者啥都行 我们这basepage就两个 一共才1000 个record
        # tail要多很多
        # 你可以随机选base page
        # 然后可以看如果这个page对应的tail page 有没有100 个record 这样， 如果不够就不merge 这样
        # 哦 那可能不用了 对 ？嗯有可能， 反正你怎么弄都可以的你弄个loop也可以
        # merge_page_index = randint(0,self.num_base_page-1)
        # time stamp column不知道要不要改， 不用的话我们直接只拿实际data的column
        #  tail rid =1,1,1,1,1,1,1,1,,1,1,1, ，base
        #  2 base, 1 base full,
        # “”“”“”从key开始就是真实data
        while True:
            for page_index in range(self.num_base_page - 1):  # pick a base page
                merge_number = 128
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
                        for i in range(self.bufferpool.get(page_in_pool_index).num_record):
                            page_data = self.bufferpool.get(page_in_pool_index).read(i)
                            page_copy.write(page_data, i)  # write page in pool to the copy
                    page_copy.dirty = True
                    base_copy[column] = page_copy
                base_lineage = page_copy.lineage  # check lineage of basepage
                tailpage_number = base_lineage / 512  # get corresponding tailpage that haven't merged
                # 这里我怕这个index用着用着他page被evict了就变成错的page了 可以
                # 这个pin我不知道怎么用 他是个numpy的东西我不太懂

                # get tail page from bufferpool and pin them
                tail_c_0_index = self.bufferpool.getindex(self.name, "t", page_index, tailpage_number, self.key)
                self.bufferpool.pinned[tail_c_0_index] = 1
                # check if tail has enough record to merge
                num_record_unmerged = self.bufferpool.get(tail_c_1_index).num_record - base_lineage
                if num_record_unmerged < merge_number:
                    self.bufferpool.pinned[tail_c_0_index] = 0
                    continue
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
                    data = self.bufferpool.get(tail_c_0_index).read(slot)
                    base_copy[0].write(slot, data)
                    data = self.bufferpool.get(tail_c_1_index).read(slot)
                    base_copy[1].write(slot, data)
                    data = self.bufferpool.get(tail_c_2_index).read(slot)
                    base_copy[2].write(slot, data)
                    data = self.bufferpool.get(tail_c_3_index).read(slot)
                    base_copy[3].write(slot, data)
                    data = self.bufferpool.get(tail_c_4_index).read(slot)
                    base_copy[4].write(slot, data)
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
                time.sleep(MERGE_INTERVAL)
                # 哩个系bufferpool的directory， 给一个page的meta return bufferpool 的index
                # table 的 page_directory 是给rid return page index 和 slot
                # 两个是不同的 copy左 5 个 真实column
                # 5洗

                # 假设 一个basepages 9 个 column
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
                # 反正你要basepage就会拿到新的copy， 要indirection就会拿到旧的
                # 你用一个page永远都会用getindex，不会care这个index是多少
                # 5,6,7,8是旧的data_column, 而且是不dirty的不会写回去
                # 但是你的新的copy 10 11 12 13 是dirty的 最后会写回去
                # 而且你的indirection是没变的还是旧的那个，update时改了也是改了旧的
                # 你去indirection还是会get到旧的这个index 所以有最新的



class StoreData:

    def __init__(self):

        self.label = ''
        self.name1 = ''
        self.name2 = ''
        self.path = ''
        self.remark = ''
        self.createdAt = None
        self.updatedAt = None

    def print(self):
        print('label [' + self.label + ']')


class ContentsData:

    def __init__(self):

        self.storeLabel = ''
        self.name = ''
        self.productNumber = ''
        self.extension = ''
        self.tag = ''
        self.publishDate = None
        self.fileDate = None
        self.fileCount = 0
        self.size = 0
        self.rating = 0
        self.comment = ''
        self.remark = ''
        self.isNotExist = 0
        self.createdAt = None
        self.updatedAt = None

    def print(self):
        print('store_label [' + self.storeLabel + ']')

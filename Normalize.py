from CreateTable import *
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials


def getMedian(alist):
    """return median of alist"""
    if alist == []:
        return []
    blist = sorted(alist, key=lambda x: (x is None, x))
    k = -1
    while blist[k] == None:
        k -= 1
    length = len(alist) + k + 1
    if length % 2 == 1:
        # length of list is odd so return middle element
        return blist[int(((length + 1) / 2) - 1)]
    else:
        # length of list is even so compute midpoint
        v1 = blist[int(length / 2)]
        v2 = blist[(int(length / 2) - 1)]
        return (v1 + v2) / 2.0

def getAbsoluteStandardDeviation(alist, median):
    """given alist and median return absolute standard deviation"""
    sum = 0
    for item in alist:
        if item != None:
            sum += abs(item - median)
    return sum / len(alist)

##################################################
###
### FINISH WRITING THIS METHOD


def normalizeColumn(service, col):
    """given a column number, normalize that column in self.data
    using the Modified Standard Score"""

    median = getMedian(col)
    asd = getAbsoluteStandardDeviation(col, median)
    for i in range(len(col)):
        if col[i] != None:
            col[i] = (col[i] - median) / asd
    return col

def normalize():
    credentials = GoogleCredentials.get_application_default()
    bigquery_service = build('bigquery', 'v2', credentials=credentials)
    listColumn = {'customer_id': [], 'sku': [], 'sales': [], 'views': [], 'carts': [], 'sales_effective_rate': [], 'rating': [], 'comments': []}
    data = result(bigquery_service)

    for row in data['rows']:
        col = []
        col += [field['v'] for field in row['f']]
        listColumn['customer_id'] += [col[0]]
        listColumn['sku'] += [col[1]]
        if col[2] is not None:
            listColumn['sales'] += [float(col[2])]
        else:
            listColumn['sales'] += [col[2]]

        if col[3] is not None:
            listColumn['views'] += [float(col[3])]
        else:
            listColumn['views'] += [col[3]]

        if col[4] is not None:
            listColumn['carts'] += [float(col[4])]
        else:
            listColumn['carts'] += [col[4]]

        if col[5] is not None:
            listColumn['sales_effective_rate'] += [float(col[5])]
        else:
            listColumn['sales_effective_rate'] += [col[5]]

        if col[6] is not None:
            listColumn['rating'] += [float(col[6])]
        else:
            listColumn['rating'] += [col[6]]

        if col[7] is not None:
            listColumn['comments'] += [float(col[7])]
        else:
            listColumn['comments'] += [col[7]]

    resultList = {}
    for col in listColumn:
        if col != 'customer_id' and col != 'sku':
            resultList[col] = normalizeColumn(bigquery_service, listColumn[col])
        else:
            resultList[col] = listColumn[col]

    return resultList

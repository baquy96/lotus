from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials


def getMedian(alist):
    """return median of alist and length of list except null value"""
    if alist == []:
        return []
    blist = sorted(alist, key=lambda x: (x is None, x))
    k = -1
    while blist[k] is None:
        k -= 1
    length = len(alist) + k + 1
    if length % 2 == 1:
        # length of list is odd so return middle element
        return (blist[int(((length + 1) / 2) - 1)], length)
    else:
        # length of list is even so compute midpoint
        v1 = blist[int(length / 2)]
        v2 = blist[(int(length / 2) - 1)]
        return ((v1 + v2) / 2.0, length)


def getAbsoluteStandardDeviation(alist, median, length):
    """given alist and median return absolute standard deviation"""
    sum = 0
    for item in alist:
        if item != None:
            sum += abs(item - median)
    return sum / length


##################################################
###
### FINISH WRITING THIS METHOD


def normalizeColumn(data, columnName):
    """given a column number, normalize that column in self.data
    using the Modified Standard Score"""
    col = [v[columnName] for v in data.values()]
    (median, length) = getMedian(col)
    asd = getAbsoluteStandardDeviation(col, median, length)
    i = 0
    for key in data.keys():
        if col[i] is not None:
            data[key][columnName] = (col[i] - median) / asd
        i += 1
    return data


def normalize(data):
    credentials = GoogleCredentials.get_application_default()
    bigquery_service = build('bigquery', 'v2', credentials=credentials)
    listColumn = {}
    columnName = ['sales', 'views', 'carts', 'sales_effective_rate', 'rating', 'comments']
    for row in data['rows']:
        col = []
        col += [field['v'] for field in row['f']]
        listColumn.setdefault((col[0], col[1]), {})

        for i in range(len(columnName)):
            if col[i+2] is not None:
                listColumn[(col[0], col[1])][columnName[i]] = float(col[i+2])
            else:
                listColumn[(col[0], col[1])][columnName[i]] = None

    for name in columnName:
        listColumn = normalizeColumn(listColumn, name)
    return listColumn
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from Normalize import normalize

frequencies = {}
deviations = {}

def computeDeviations(data):

    for (item, rating) in data.items():
        if item != 'customer_id' and item != 'sku':
            frequencies.setdefault(item, {})
            deviations.setdefault(item, {})
            for (item2, rating2) in data.items():
                if item != item2 and item2 != 'customer_id' and item2 != 'sku':
                    frequencies[item].setdefault(item2, 0)
                    deviations[item].setdefault(item2, 0.0)
                    for i in range(len(rating)):
                        if rating[i] is not None and rating2[i] is not None:
                            frequencies[item][item2] += 1
                            deviations[item][item2] += rating[i] - rating2[i]

    for (item, ratings) in deviations.items():
        for item2 in ratings:
            ratings[item2] /= frequencies[item][item2]

def slopeOneRecommendations(data):
    recommendations = {}
    frequencies = {}
    # for every item and rating in the user's recommendations
    for (userItem, userRating) in data.items():
        # for every item in our dataset that the user didn't rate
        for (diffItem, diffRatings) in deviations.items():
            if diffItem not in userRatings and userItem in deviations[diffItem]:
                freq = frequencies[diffItem][userItem]
                recommendations.setdefault(diffItem, 0.0)
                frequencies.setdefault(diffItem, 0)
                # add to the running sum representing the numerator
                # of the formula
                recommendations[diffItem] += (diffRatings[userItem] + userRating) * freq
                # keep a running sum of the frequency of diffitem
                frequencies[diffItem] += freq
    recommendations = [(convertProductID2name(k),
                        v / frequencies[k])
                       for (k, v) in recommendations.items()]
    # finally sort and return
    recommendations.sort(key=lambda artistTuple: artistTuple[1],
                         reverse=True)
    # I am only going to return the first 50 recommendations
    return recommendations[:50]

def predict():
    data = normalize()
    computeDeviations(data)

credentials = GoogleCredentials.get_application_default()
bigquery_service = build('bigquery', 'v2', credentials=credentials)
predict()
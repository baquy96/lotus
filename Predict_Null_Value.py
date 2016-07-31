frequencies = {}
deviations = {}


def computeDeviations(data):
    # for each person in the data:
    #    get their ratings
    for ratings in data.values():
        # for each item & rating in that set of ratings:
        for (item, rating) in ratings.items():
            if rating is not None:
                frequencies.setdefault(item, {})
                deviations.setdefault(item, {})
                # for each item2 & rating2 in that set of ratings:
                for (item2, rating2) in ratings.items():
                    if item != item2 and rating2 is not None:
                        # add the difference between the ratings to our
                        # computation
                        frequencies[item].setdefault(item2, 0)
                        deviations[item].setdefault(item2, 0.0)
                        frequencies[item][item2] += 1
                        deviations[item][item2] += rating - rating2

    for (item, ratings) in deviations.items():
        for item2 in ratings:
            ratings[item2] /= frequencies[item][item2]


def slopeOneRecommendations(userRatings):
    recommendations = {}
    frequencies__ = {}
    # for every item and rating in the user's recommendations
    for (userItem, userRating) in userRatings.items():
        # for every item in our dataset that the user didn't rate
        if userRating is not None:
            for (diffItem, diffRatings) in deviations.items():
                if userRatings[diffItem] is None and \
                                userItem in deviations[diffItem]:
                    freq = frequencies[diffItem][userItem]
                    recommendations.setdefault(diffItem, 0.0)
                    frequencies__.setdefault(diffItem, 0)
                    # add to the running sum representing the numerator
                    # of the formula
                    recommendations[diffItem] += (diffRatings[userItem] +
                                                  userRating) * freq
                    # keep a running sum of the frequency of diffitem
                    frequencies__[diffItem] += freq
    recommendations = [(k, v / frequencies__[k])
                       for (k, v) in recommendations.items()]
    # finally sort and return
    recommendations.sort(key=lambda artistTuple: artistTuple[1],
                         reverse=True)
    # I am only going to return the first 50 recommendations
    return recommendations


def predict(data):
    computeDeviations(data)
    for key in data.keys():
        res = slopeOneRecommendations(data[key])
        if res != []:
            for (userItem, userRating) in res:
                data[key][userItem] = userRating
    return data
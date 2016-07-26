from Predict_Null_Value import predict


def pearson(rating1, rating2):
    sum_xy = 0
    sum_x = 0
    sum_y = 0
    sum_x2 = 0
    sum_y2 = 0
    n = 0
    for key in rating1:
        if key in rating2:
            n += 1
            x = rating1[key]
            y = rating2[key]
            sum_xy += x * y
            sum_x += x
            sum_y += y
            sum_x2 += pow(x, 2)
            sum_y2 += pow(y, 2)
    if n == 0:
        return 0
    # now compute denominator
    denominator = sqrt(sum_x2 - pow(sum_x, 2) / n) * \
                  sqrt(sum_y2 - pow(sum_y, 2) / n)
    if denominator == 0:
        return 0
    else:
        return (sum_xy - (sum_x * sum_y) / n) / denominator


def computeNearestNeighbor(data, username):
    """creates a sorted list of users based on their distance
    to username"""
    distances = []
    for instance in self.data:
        if instance != username:
            distance = pearson(data[username],
                               data[instance])
            distances.append((instance, distance))
    # sort based on distance -- closest first
    distances.sort(key=lambda artistTuple: artistTuple[1],
                   reverse=True)
    return distances


def recommend(user, num):
    """Give list of recommendations"""
    recommendations = {}
    data = predict()
    # first get list of users  ordered by nearness
    nearest = self.computeNearestNeighbor(data, user)
    #
    # now get the ratings for the user
    #
    userRatings = data[user]
    #
    # determine the total distance
    totalDistance = 0.0
    for i in range(num):
        totalDistance += nearest[i][1]
    # now iterate through the k nearest neighbors
    # accumulating their ratings
    for i in range(num):
        # compute slice of pie
        weight = nearest[i][1] / totalDistance
        # get the name of the person
        name = nearest[i][0]
        # get the ratings for this person
        neighborRatings = data[name]
        # get the name of the person
        # now find bands neighbor rated that user didn't
        for artist in neighborRatings:
            if not artist in userRatings:
                if artist not in recommendations:
                    recommendations[artist] = neighborRatings[artist] * \
                                              weight
                else:
                    recommendations[artist] = recommendations[artist] + \
                                              neighborRatings[artist] * \
                                              weight
    # now make list from dictionary and only get the first n items
    recommendations = list(recommendations.items())[:self.n]
    recommendations = [(self.convertProductID2name(k), v)
                       for (k, v) in recommendations]
    # finally sort and return
    recommendations.sort(key=lambda artistTuple: artistTuple[1],
                         reverse=True)
    return recommendations

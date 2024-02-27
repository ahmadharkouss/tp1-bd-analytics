from mrjob.job import MRJob


class BaseballFriends(MRJob):

    def mapper(self, _, line):
        name, team, friends_str = line.split(',', 2)
        friends = friends_str.split(',')
        friends = [friend.strip() for friend in friends]
        yield team.strip(), (name.strip(), friends)

    def reducer(self, team, name_friends_list):
        name_friends_list = list(name_friends_list)
        fans = []
        for nf in name_friends_list:
            fans.append(nf[0])

        for nf in name_friends_list:
            result = []
            result.append(team)
            name, friends = nf[0], nf[1]
            for f in friends:
                if f in fans:
                    result.append(f)
            yield name, result


if __name__ == '__main__':
    BaseballFriends.run()


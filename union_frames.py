from functools import reduce


class UnionFrames:
    @staticmethod
    def create(frames):
        return reduce(
            lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True),
            frames,
        ).distinct()

import math
class Solution:
    def minEatingSpeed(self, piles: List[int], h: int) -> int:
        l, r = 1, max(piles)
        result = r
        if h == len(piles):
            return result
        while l <= r:
            time = 0
            speed = (l+r) // 2
            for pile in piles:
                time += math.ceil(pile / speed)
            if time <= h:
                result = speed
                r = speed - 1
            else:
                l = speed + 1
        return result
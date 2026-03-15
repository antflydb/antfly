from enum import Enum


class SignificanceAlgorithm(str, Enum):
    CHI_SQUARED = "chi_squared"
    JLH = "jlh"
    MUTUAL_INFORMATION = "mutual_information"
    PERCENTAGE = "percentage"

    def __str__(self) -> str:
        return str(self.value)

class BaseTaskiqSQLAlchemyException(Exception):
    """
    Base error for all exceptions in the library.
    """


class ResultIsMissingError(BaseTaskiqSQLAlchemyException):
    """
    Error if cannot fetch result from database.
    """

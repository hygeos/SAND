from time import sleep


def request_get(session, url, **kwargs):
    r = session.get(url, **kwargs)
    for _ in range(10):
        try:
            raise_api_error(r)
        except RateLimitError:
            sleep(3)
            r = session.get(url, **kwargs)
    return r

def raise_api_error(response: dict):
    assert hasattr(response,'status_code')
    status = response.status_code

    if status == 401:
        raise UnauthorizedError
    if status == 404:
        raise FileNotFoundError
    if status == 429:
        raise RateLimitError
    
    if status//100 == 3:
        raise RedirectionError
    if status//100 == 4:
        raise InvalidParametersError
    if status//100 == 5:
        raise ServerError


class InvalidParametersError(Exception):
    """Provided parameters are invalid."""
    pass

class UnauthorizedError(Exception):
    """User does not have access to the requested endpoint."""
    pass

class RateLimitError(Exception):
    """Account does not support multiple requests at a time."""
    pass

class RedirectionError(Exception):
    """Account does not support multiple requests at a time."""
    pass

class ServerError(Exception):
    """The server failed to fulfil a request."""
    pass
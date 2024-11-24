from .logger import PandabaseLogger

## create singleton instance ##
logger = PandabaseLogger()

## expose the logger object ##
__all__ = ['logger']
from pathlib import Path
import os
import platform

_SYSTEM = platform.system()


def user_home() -> Path:
    return Path('~').expanduser()


def user_config_path() -> Path:
    e = os.environ.get('XDG_CONFIG_HOME')
    if e:
        return Path(e)
    if _SYSTEM == 'Windows':
        e = os.environ.get('AppData')
        if e:
            return Path(e)
    if _SYSTEM == 'Darwin':
        return user_home() / 'Library/Preferences'
    return user_home() / '.config'


def user_cache_path() -> Path:
    e = os.environ.get('XDG_CACHE_HOME')
    if e:
        return Path(e)
    if _SYSTEM == 'Windows':
        e = os.environ.get('LocalAppData')
        if e:
            return Path(e)
    if _SYSTEM == 'Darwin':
        return user_home() / 'Library/Caches'
    return user_home() / '.cache'

__version__ = "0.41.0.1"

__changelog__ = {
    "unreleased":"""""",
    "0.42":
    """
        - Origin -> EA Desktop
            -> reworked all necessary functions using the new API
            -> deprecation message on certain functions not used in EA Desktop (still using the Origin API for now)
            -> rewrote file size discovery function (folder size)
    """,
    "0.41.1":
    """
        - Origin -> EA Desktop
            -> fixes login while switching from the Origin page to the EA Desktop page (uses a certain juno endpoint to prevent using pc_sign)
            -> fixes errors linked to that change
            -> fixes game library discovery and verifying the installed games
            -> implements a new decryption system in order to get the game library
            -> map.crc -> map.eacrc
        - update Galaxy API version to 0.69
    """,
    "0.40":
    """
        - `get_local_size`: return `None` if map.crc not found instead of raising error
        - fix detecting installed launcher & games when EA Desktop is installed
    """,
    "0.39":
    """
        - update Galaxy API version to 0.68
        - help with adding subscription games to user library when clicking Install
        - add missing randomization to api[1-4].origin.com when fetching subscription games
    """,
    "0.38":
    """
        - add ability to launch Origin games bought in external stores (#30 by @claushofmann + further changes)
        - fix parsing games manifest files and handled files with invalid content
        - refactor `get_subscription_games` and `get_game_library_settings`
    """,
    "0.37.1":
    """
        - fix getting subscription with 'enable' status. Bug related with issue: (#18)
    """,
    "0.37":
    """
        - rename Origin Access [Premium] to EA Play [Pro]
        - fix crash if ProgramData is undefined in Environmental variables (#23 by @NathanaelA)
    """,
    "0.36":
    """
        - better handle installation status of games
        - fix error on retrieving achievements for some games
        - added support for local sizes
    """,
    "0.35":
    """
        - added support for subscriptions
    """,
    "0.34.1":
    """
        - add extended logging to find session expiration time mechanism
    """,
    "0.34":
    """
        - fix rare bug while parsing game times (#16)
        - fix handling status 400 with "login_error": go to "Credentials Lost" instead of "Offline. Retry"
    """
}

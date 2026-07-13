__version__ = "0.51.0"
__branch__ = "galaxy-2.1"
__changelog__ = {
    "unreleased":"""""",
    "0.51.0":"""
        - conform ourselves to new Galaxy 2.1 API regarding subscription games list
        - strengthen the login process to prevent token expiration
        - simplify pc_sign generation, improve data gathering [thx Maxima project]
        - review code_verifier logic according to oauth2 spec
        - prevent the authentification system from hanging up if no pcsign is found
        - fix user presence parsing
    """,
    "0.50.0":"""
        - added full support for Galaxy 2.1
        - bumped code to Python 3.13
    """,
    "0.44.6":
    """
        - implemented user presence parsing and calls to Galaxy (thanks to the Maxima project)
        - added protobufs for presence parsing (see src/generated_protos)
        - fixed the subscription games not properly loading due to EA API changes
        - minor code fixups and improvements
    """,
    "0.44.5":
    """
        - fixed subscription gathering
        - improved game discovery
        - actually check game time and achievements for subbed games.
    """,
    "0.44.4":
    """
        - plugin has been reworked entirely for better maintainability
        - fixes game discovery, fixes local game detection
        - added background prefetching of offers for better local game detection
        - improved error handling during offer prefetching
        - improved overall performance and responsiveness
    """,
    "0.44.3":
    """
        - made the authentication process go snappier thanks to a hw cache for the pc_sign
        - fixed the mid creation method linked to the pc_sign creation (thanks to @imLinguin for the help)
        - normalized the local game discovery process (instead of going to billions of places, just load installerdata.xml). if the game is too old, keep the old method
        - fixed the local game size discovery (use of manifest file)
        - fixed a few quacks related to code
        - removed the use of deprecated Galaxy methods
    """,
    "0.44.2":
    """
        - improve game library discovery
        - improve local game discovery
        - properly handle the case when the offer points to a xml file instead of an executable
    """,
    "0.44":
    """
        - overall improvements to the plugin
        - implement pc_sign, and use it for login
        - properly handle the login refresh process, and the token expiration
    """,
    "0.43":
    """
        - rewrote the login process to get a long-lasting token, with a token refresh method. Kudos to @imLinguin for the help and information
        - rewrote the local game discovery and status
        - split the HTTP Client from the backend onto a separate file
        - ditch the use of the Galaxy HTTP platform to use aiohttp (per API recommendations v69)
    """,
    "0.42":
    """
        - Origin -> EA Desktop
            -> reworked all functions using the new API
            -> removed deprecated functions (not used in EA Desktop)
            -> rewrote file size discovery function (folder size)
            -> rewrote local game discovery and status (no need of IS file decryption)
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

# EA Desktop (ead) integration for GOG Galaxy

Allows to import your list of games, achievements and time played.
Forked from FriendsOfGalaxy.

# Information

You are currently on the "juno_api" branch, which is a rewritten version of that same plugin, using the new Juno API for EA Desktop.
Achievements, game play time and available subscription games should be up to date on this specific version.

## Disclaimer

Please note that, in order to make this plugin fully functional, you need to have a valid EA Desktop account.
Mac users still have Origin, and will be switched to EA Desktop pretty soon.

## Disclaimer 2

While the plugin is proven to work on Windows, I *cannot confirm* that this plugin also works on Mac.

## Installation

*MAKE SURE GOG GALAXY IS CLOSED BEFORE PROCEEDING TO THE INSTALLATION.
Please note that for the "install.bat" file to work (in it's current state), you will need to have Python 3.7 (and pip) installed.
Why 3.7 you may ask ? Because Galaxy uses that particular version, and certain dependencies will only work in said version.*

1. Download the plugin from the [releases page](https://github.com/BellezaEmporium/galaxy-integration-ead/releases).
2. Unzip the archive.
3. (Do this only if you have Python installed. If you don't, do the secondary option) On the unzipped directory, use the "install.bat" file to automate the process OR extract the contents of the file into the Origin plugin's location : origin_7f53219b-4e2b-4591-9f4f-dfc5f4ba9eb0.
4. Start GOG Galaxy

## Configuration

Just login to your EA Desktop account from GOG Galaxy, and enjoy your games.

## Kudos

- [@imLinguin](https://github.com/imLinguin) for helping me on the EA login logic & various hints
- [@Nutzzz](https://github.com/Nutzzz) for describing me the IS file decryption and testing the plugin on various aspects.
- All the plugin testers
- The GOG Cafe / Wing of GOG Discord servers

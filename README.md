# EA Desktop (ead) integration for GOG Galaxy

Allows to import your list of games, achievements and time played.
Forked from FriendsOfGalaxy.

# Information

This plugin is made to work with the new EA Desktop program. Since Origin has sunsetted in April 17th, 2025, you'll only be able to use this version of the plugin (see the branch name).
If you're migrating from Origin to EA, you might encounter an offer conflict, since the old offer structure is different to the one we scrape today in the EA API.

If you encounter such issues (like games not showing up, overall quacks talking about displayName or such stuff), you need to force the cache update.
To do so, you can force the cache update by modifying lines 340-347 of the plugin.py file, and replace them with the following: :

```
for offer_id in offer_ids:
  missing_offers.append(offer_id)
```

## Disclaimer

Please note that, in order to make this plugin fully functional, you need to have a valid EA Desktop account.

## Disclaimer 2

While the plugin is proven to work on Windows, I *cannot confirm* that this plugin also works on Mac.

## Installation

1. Download the plugin from the [releases page](https://github.com/BellezaEmporium/galaxy-integration-ead/releases).
2. Unzip the archive.
3. Extract the contents of the file into the Origin plugin's location : origin_7f53219b-4e2b-4591-9f4f-dfc5f4ba9eb0.
4. Start GOG Galaxy

## Configuration

Just login to your EA Desktop account from GOG Galaxy, and enjoy your games.

## Kudos

- [@imLinguin](https://github.com/imLinguin) for helping me on the EA login logic & various hints
- [@Nutzzz](https://github.com/Nutzzz) for describing me the IS file decryption and testing the plugin on various aspects.
- [The Playnite community](https://github.com/JosefNemec/Playnite), you guys rock !
- All the plugin testers
- The GOG Cafe / Wing of GOG Discord servers

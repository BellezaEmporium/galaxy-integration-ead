# EA Desktop (ead) integration for GOG Galaxy

Allows to import your list of games, achievements and time played.
Forked from FriendsOfGalaxy.

# Information

This plugin is made to work with the new EA Desktop program. Since Origin has sunsetted in April 17th, 2025, you'll only be able to use this version of the plugin (see the branch name).
If you're migrating from Origin to EA, you might encounter an offer conflict, since the old offer structure is different to the one we scrape today in the EA API.

If you encounter such issues (like games not showing up, overall quacks talking about displayName or such stuff), you need to force the cache update.
To do so, search for a function called "get_offers". The function should begin like this :

```
for offer_id in offer_ids:
    cached_offer = self._offer_id_cache.get(offer_id)
    if isinstance(cached_offer, dict):
        offers[offer_id] = cached_offer
    else:
        missing_offers.append(offer_id)
```

Replace these lines with this (and keep a backup of the lines earlier on) :

```
for offer_id in offer_ids:
    missing_offers.append(offer_id)
```

Make sure the line after the for function is properly indented, as the plugin wouldn't work without that condition met.
Load Galaxy afterwards, and reload the plugin. Once you've confirmed the games are now properly updated, you can reinstate the lines back.

UPDATED 11/2025 : Starting with Windows 11, version 25H2, Microsoft has deprecated and removed both WMIC (Windows Management Instrumentation Command-line) and PowerShell 2.0 from the operating system. Although the provided PowerShell script (.ps1) remains compatible, users may encounter errors after the update if the script attempts to invoke WMIC, as the utility is no longer included in this release.

## Disclaimer

Please note that, in order to make this plugin fully functional, you need to have a valid EA Desktop account.

## Disclaimer 2

While the plugin is proven to work on Windows, I *cannot confirm* that this plugin also works on Mac.

## Installation

1. Download the plugin from the [releases page](https://github.com/BellezaEmporium/galaxy-integration-ead/releases). You'll have multiple versions, one for Windows, one for macOS, and the Galaxy autoupgrade package. The Galaxy autoupgrade package is here so that GOG Galaxy can automatically upgrade the plugin by itself. If it's your first time with the plugin, it's recommended to use the OS-targeted releases.
2. Unzip the archive.
3. If you picked one of the OS-targeted releases, you can just use the "install" file, that will automatically unzip and move the contents of the plugin to the right place. Otherwise, extract the contents of the file into Origin's plugin location : origin_7f53219b-4e2b-4591-9f4f-dfc5f4ba9eb0.
4. Start GOG Galaxy, and enjoy !

## Configuration

Just login to your EA Desktop account from GOG Galaxy, and enjoy your games.

## Kudos

- [@imLinguin](https://github.com/imLinguin) for helping me on the EA login logic & various hints
- [@Nutzzz](https://github.com/Nutzzz) for describing me the IS file decryption and testing the plugin on various aspects
- [The Playnite community](https://github.com/JosefNemec/Playnite), you guys rock !
- All the plugin testers
- The GOG Cafe / Wing of GOG Discord servers

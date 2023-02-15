package net.william278.husksync.migrator;

import com.zaxxer.hikari.HikariDataSource;
import net.william278.husksync.BukkitHuskSync;
import net.william278.husksync.data.*;
import net.william278.husksync.player.User;
import net.william278.mpdbconverter.MPDBConverter;
import org.bukkit.Bukkit;
import org.bukkit.event.inventory.InventoryType;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.Plugin;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.regex.Pattern;

/**
 * A migrator for migrating MySQLPlayerDataBridge data to HuskSync {@link UserData}
 */
public class MpdbMigrator extends Migrator {

    private final MPDBConverter mpdbConverter;
    private String sourceHost;
    private int sourcePort;
    private String sourceUsername;
    private String sourcePassword;
    private String sourceDatabase;
    private String sourceInventoryTable;
    private String sourceEnderChestTable;
    private String sourceExperienceTable;
    private final String minecraftVersion;

    public MpdbMigrator(@NotNull BukkitHuskSync plugin, @NotNull Plugin mySqlPlayerDataBridge) {
        super(plugin);
        this.mpdbConverter = MPDBConverter.getInstance(mySqlPlayerDataBridge);
        this.sourceHost = plugin.getSettings().mySqlHost;
        this.sourcePort = plugin.getSettings().mySqlPort;
        this.sourceUsername = plugin.getSettings().mySqlUsername;
        this.sourcePassword = plugin.getSettings().mySqlPassword;
        this.sourceDatabase = plugin.getSettings().mySqlDatabase;
        this.sourceInventoryTable = "mpdb_inventory";
        this.sourceEnderChestTable = "mpdb_enderchest";
        this.sourceExperienceTable = "mpdb_experience";
        this.minecraftVersion = plugin.getMinecraftVersion().toString();

    }

    @Override
    public CompletableFuture<Boolean> start() {
        plugin.log(Level.INFO, "Starting migration from MySQLPlayerDataBridge to HuskSync...");
        final long startTime = System.currentTimeMillis();
        return CompletableFuture.supplyAsync(() -> {
            // Wipe the existing database, preparing it for data import
            plugin.log(Level.INFO, "Preparing existing database (wiping)...");
            plugin.getDatabase().wipeDatabase().join();
            plugin.log(Level.INFO, "Successfully wiped user data database (took " + (System.currentTimeMillis() - startTime) + "ms)");

            // Create jdbc driver connection url
            final String jdbcUrl = "jdbc:mysql://" + sourceHost + ":" + sourcePort + "/" + sourceDatabase;

            // Create a new data source for the mpdb converter
            try (final HikariDataSource connectionPool = new HikariDataSource()) {
                plugin.log(Level.INFO, "Establishing connection to MySQLPlayerDataBridge database...");
                connectionPool.setJdbcUrl(jdbcUrl);
                connectionPool.setUsername(sourceUsername);
                connectionPool.setPassword(sourcePassword);
                connectionPool.setPoolName((getIdentifier() + "_migrator_pool").toUpperCase());

                plugin.log(Level.INFO, "Downloading raw data from the MySQLPlayerDataBridge database (this might take a while)...");
                final List<MpdbData> dataToMigrate = new ArrayList<>();
                try (final Connection connection = connectionPool.getConnection()) {
                    try (final PreparedStatement statement = connection.prepareStatement(("SELECT `%source_inventory_table%`.`player_uuid`, `%source_inventory_table%`.`player_name`, `inventory`, `armor`, `enderchest`, `exp_lvl`, `exp`, `total_exp`\n" +
                                                                                          "FROM `%source_inventory_table%`\n" +
                                                                                          "    INNER JOIN `%source_ender_chest_table%`\n" +
                                                                                          "        ON `%source_inventory_table%`.`player_uuid` = `%source_ender_chest_table%`.`player_uuid`\n" +
                                                                                          "    INNER JOIN `%source_xp_table%`\n" +
                                                                                          "        ON `%source_inventory_table%`.`player_uuid` = `%source_xp_table%`.`player_uuid`;\n").replaceAll(Pattern.quote("%source_inventory_table%"), sourceInventoryTable)
                            .replaceAll(Pattern.quote("%source_ender_chest_table%"), sourceEnderChestTable)
                            .replaceAll(Pattern.quote("%source_xp_table%"), sourceExperienceTable))) {
                        try (final ResultSet resultSet = statement.executeQuery()) {
                            int playersMigrated = 0;
                            while (resultSet.next()) {
                                dataToMigrate.add(new MpdbData(
                                        new User(UUID.fromString(resultSet.getString("player_uuid")),
                                                resultSet.getString("player_name")),
                                        resultSet.getString("inventory"),
                                        resultSet.getString("armor"),
                                        resultSet.getString("enderchest"),
                                        resultSet.getInt("exp_lvl"),
                                        resultSet.getInt("exp"),
                                        resultSet.getInt("total_exp")
                                ));
                                playersMigrated++;
                                if (playersMigrated % 25 == 0) {
                                    plugin.log(Level.INFO, "Downloaded MySQLPlayerDataBridge data for " + playersMigrated + " players...");
                                }
                            }
                        }
                    }
                }
                plugin.log(Level.INFO, "Completed download of " + dataToMigrate.size() + " entries from the MySQLPlayerDataBridge database!");
                plugin.log(Level.INFO, "Converting raw MySQLPlayerDataBridge data to HuskSync user data (this might take a while)...");

                final AtomicInteger playersConverted = new AtomicInteger();
                dataToMigrate.forEach(data -> data.toUserData(mpdbConverter, minecraftVersion).thenAccept(convertedData -> {
                    plugin.getDatabase().ensureUser(data.user()).thenRun(() ->
                                    plugin.getDatabase().setUserData(data.user(), convertedData, DataSaveCause.MPDB_MIGRATION))
                            .exceptionally(exception -> {
                                plugin.log(Level.SEVERE, "Failed to migrate MySQLPlayerDataBridge data for " + data.user().username + ": " + exception.getMessage());
                                return null;
                            }).join();
                    playersConverted.getAndIncrement();
                    if (playersConverted.get() % 50 == 0) {
                        plugin.log(Level.INFO, "Converted MySQLPlayerDataBridge data for " + playersConverted + " players...");
                    }
                }).join());
                plugin.log(Level.INFO, "Migration complete for " + dataToMigrate.size() + " users in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds!");
                return true;
            } catch (Exception e) {
                plugin.log(Level.SEVERE, "Error while migrating data: " + e.getMessage() + " - are your source database credentials correct?");
                return false;
            }
        });
    }

    @Override
    public void handleConfigurationCommand(@NotNull String[] args) {
        if (args.length == 2) {
            boolean b;
            switch (args[0].toLowerCase()) {
                case "host":
                    this.sourceHost = args[1];
                    b = true;
                    break;
                case "port":
                    try {
                        this.sourcePort = Integer.parseInt(args[1]);
                        b = true;
                        break;
                    } catch (NumberFormatException e) {
                        b = false;
                        break;
                    }
                case "username":
                    this.sourceUsername = args[1];
                    b = true;
                    break;
                case "password":
                    this.sourcePassword = args[1];
                    b = true;
                    break;
                case "database":
                    this.sourceDatabase = args[1];
                    b = true;
                    break;
                case "inventory_table":
                    this.sourceInventoryTable = args[1];
                    b = true;
                    break;
                case "ender_chest_table":
                    this.sourceEnderChestTable = args[1];
                    b = true;
                    break;
                case "experience_table":
                    this.sourceExperienceTable = args[1];
                    b = true;
                    break;
                default:
                    b = false;
                    break;
            }
            if (b) {
                plugin.log(Level.INFO, getHelpMenu());
                plugin.log(Level.INFO, "Successfully set " + args[0] + " to " +
                                                           obfuscateDataString(args[1]));
            } else {
                plugin.log(Level.INFO, "Invalid operation, could not set " + args[0] + " to " +
                                                           obfuscateDataString(args[1]) + " (is it a valid option?)");
            }
        } else {
            plugin.log(Level.INFO, getHelpMenu());
        }
    }

    @NotNull
    @Override
    public String getIdentifier() {
        return "mpdb";
    }

    @NotNull
    @Override
    public String getName() {
        return "MySQLPlayerDataBridge Migrator";
    }

    @NotNull
    @Override
    public String getHelpMenu() {
        return ("=== MySQLPlayerDataBridge Migration Wizard ==========\n" +
                "This will migrate inventories, ender chests and XP\n" +
                "from the MySQLPlayerDataBridge plugin to HuskSync.\n" +
                "\n" +
                "To prevent excessive migration times, other non-vital\n" +
                "data will not be transferred.\n" +
                "\n" +
                "[!] Existing data in the database will be wiped. [!]\n" +
                "\n" +
                "STEP 1] Please ensure no players are on any servers.\n" +
                "\n" +
                "STEP 2] HuskSync will need to connect to the database\n" +
                "used to hold the source MySQLPlayerDataBridge data.\n" +
                "Please check these database parameters are OK:\n" +
                "- host: %source_host%\n" +
                "- port: %source_port%\n" +
                "- username: %source_username%\n" +
                "- password: %source_password%\n" +
                "- database: %source_database%\n" +
                "- inventory_table: %source_inventory_table%\n" +
                "- ender_chest_table: %source_ender_chest_table%\n" +
                "- experience_table: %source_xp_table%\n" +
                "If any of these are not correct, please correct them\n" +
                "using the command:\n" +
                "\"husksync migrate mpdb set <parameter> <value>\"\n" +
                "(e.g.: \"husksync migrate mpdb set host 1.2.3.4\")\n" +
                "\n" +
                "STEP 3] HuskSync will migrate data into the database\n" +
                "tables configures in the config.yml file of this\n" +
                "server. Please make sure you're happy with this\n" +
                "before proceeding.\n" +
                "\n" +
                "STEP 4] To start the migration, please run:\n" +
                "\"husksync migrate mpdb start\"\n").replaceAll(Pattern.quote("%source_host%"), obfuscateDataString(sourceHost))
                .replaceAll(Pattern.quote("%source_port%"), Integer.toString(sourcePort))
                .replaceAll(Pattern.quote("%source_username%"), obfuscateDataString(sourceUsername))
                .replaceAll(Pattern.quote("%source_password%"), obfuscateDataString(sourcePassword))
                .replaceAll(Pattern.quote("%source_database%"), sourceDatabase)
                .replaceAll(Pattern.quote("%source_inventory_table%"), sourceInventoryTable)
                .replaceAll(Pattern.quote("%source_ender_chest_table%"), sourceEnderChestTable)
                .replaceAll(Pattern.quote("%source_xp_table%"), sourceExperienceTable);
    }

    /**
     * Represents data exported from the MySQLPlayerDataBridge source database
     */
        private static final class MpdbData {
        @NotNull
        private final User user;
        @NotNull
        private final String serializedInventory;
        @NotNull
        private final String serializedArmor;
        @NotNull
        private final String serializedEnderChest;
        private final int expLevel;
        private final float expProgress;
        private final int totalExp;

        /**
         * @param user                 The user whose data is being migrated
         * @param serializedInventory  The serialized inventory data
         * @param serializedArmor      The serialized armor data
         * @param serializedEnderChest The serialized ender chest data
         * @param expLevel             The player's current XP level
         * @param expProgress          The player's current XP progress
         * @param totalExp             The player's total XP score
         */
        private MpdbData(@NotNull User user, @NotNull String serializedInventory,
                         @NotNull String serializedArmor, @NotNull String serializedEnderChest,
                         int expLevel, float expProgress, int totalExp) {
            this.user = user;
            this.serializedInventory = serializedInventory;
            this.serializedArmor = serializedArmor;
            this.serializedEnderChest = serializedEnderChest;
            this.expLevel = expLevel;
            this.expProgress = expProgress;
            this.totalExp = totalExp;
        }

        /**
             * Converts exported MySQLPlayerDataBridge data into HuskSync's {@link UserData} object format
             *
             * @param converter The {@link MPDBConverter} to use for converting to {@link ItemStack}s
             * @return A {@link CompletableFuture} that will resolve to the converted {@link UserData} object
             */
            @NotNull
            public CompletableFuture<UserData> toUserData(@NotNull MPDBConverter converter,
                                                          @NotNull String minecraftVersion) {
                return CompletableFuture.supplyAsync(() -> {
                    // Combine inventory and armour
                    final Inventory inventory = Bukkit.createInventory(null, InventoryType.PLAYER);
                    inventory.setContents(converter.getItemStackFromSerializedData(serializedInventory));
                    final ItemStack[] armor = converter.getItemStackFromSerializedData(serializedArmor).clone();
                    for (int i = 36; i < 36 + armor.length; i++) {
                        inventory.setItem(i, armor[i - 36]);
                    }

                    // Create user data record
                    return UserData.builder(minecraftVersion)
                        .setStatus(new StatusData(20, 20, 0, 20, 10,
                            1, 0, totalExp, expLevel, expProgress, "SURVIVAL",
                            false))
                        .setInventory(new ItemData(BukkitSerializer.serializeItemStackArray(inventory.getContents()).join()))
                        .setEnderChest(new ItemData(BukkitSerializer.serializeItemStackArray(converter
                            .getItemStackFromSerializedData(serializedEnderChest)).join()))
                        .build();
                });
            }

        @NotNull
        public User user() {
            return user;
        }

        @NotNull
        public String serializedInventory() {
            return serializedInventory;
        }

        @NotNull
        public String serializedArmor() {
            return serializedArmor;
        }

        @NotNull
        public String serializedEnderChest() {
            return serializedEnderChest;
        }

        public int expLevel() {
            return expLevel;
        }

        public float expProgress() {
            return expProgress;
        }

        public int totalExp() {
            return totalExp;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (MpdbData) obj;
            return Objects.equals(this.user, that.user) &&
                   Objects.equals(this.serializedInventory, that.serializedInventory) &&
                   Objects.equals(this.serializedArmor, that.serializedArmor) &&
                   Objects.equals(this.serializedEnderChest, that.serializedEnderChest) &&
                   this.expLevel == that.expLevel &&
                   Float.floatToIntBits(this.expProgress) == Float.floatToIntBits(that.expProgress) &&
                   this.totalExp == that.totalExp;
        }

        @Override
        public int hashCode() {
            return Objects.hash(user, serializedInventory, serializedArmor, serializedEnderChest, expLevel, expProgress, totalExp);
        }

        @Override
        public String toString() {
            return "MpdbData[" +
                   "user=" + user + ", " +
                   "serializedInventory=" + serializedInventory + ", " +
                   "serializedArmor=" + serializedArmor + ", " +
                   "serializedEnderChest=" + serializedEnderChest + ", " +
                   "expLevel=" + expLevel + ", " +
                   "expProgress=" + expProgress + ", " +
                   "totalExp=" + totalExp + ']';
        }

        }

}

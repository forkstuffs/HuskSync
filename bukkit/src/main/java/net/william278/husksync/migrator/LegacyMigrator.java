package net.william278.husksync.migrator;

import com.zaxxer.hikari.HikariDataSource;
import me.william278.husksync.bukkit.data.DataSerializer;
import net.william278.hslmigrator.HSLConverter;
import net.william278.husksync.HuskSync;
import net.william278.husksync.data.*;
import net.william278.husksync.player.User;
import org.bukkit.Material;
import org.bukkit.Statistic;
import org.bukkit.entity.EntityType;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LegacyMigrator extends Migrator {

    private final HSLConverter hslConverter;
    private String sourceHost;
    private int sourcePort;
    private String sourceUsername;
    private String sourcePassword;
    private String sourceDatabase;
    private String sourcePlayersTable;
    private String sourceDataTable;

    private final String minecraftVersion;

    public LegacyMigrator(@NotNull HuskSync plugin) {
        super(plugin);
        this.hslConverter = HSLConverter.getInstance();
        this.sourceHost = plugin.getSettings().mySqlHost;
        this.sourcePort = plugin.getSettings().mySqlPort;
        this.sourceUsername = plugin.getSettings().mySqlUsername;
        this.sourcePassword = plugin.getSettings().mySqlPassword;
        this.sourceDatabase = plugin.getSettings().mySqlDatabase;
        this.sourcePlayersTable = "husksync_players";
        this.sourceDataTable = "husksync_data";
        this.minecraftVersion = plugin.getMinecraftVersion().toString();
    }

    @Override
    public CompletableFuture<Boolean> start() {
        plugin.log(Level.INFO, "Starting migration of legacy HuskSync v1.x data...");
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
                plugin.log(Level.INFO, "Establishing connection to legacy database...");
                connectionPool.setJdbcUrl(jdbcUrl);
                connectionPool.setUsername(sourceUsername);
                connectionPool.setPassword(sourcePassword);
                connectionPool.setPoolName((getIdentifier() + "_migrator_pool").toUpperCase());

                plugin.log(Level.INFO, "Downloading raw data from the legacy database (this might take a while)...");
                final List<LegacyData> dataToMigrate = new ArrayList<>();
                try (final Connection connection = connectionPool.getConnection()) {
                    try (final PreparedStatement statement = connection.prepareStatement(("SELECT `uuid`, `username`, `inventory`, `ender_chest`, `health`, `max_health`, `health_scale`, `hunger`, `saturation`, `saturation_exhaustion`, `selected_slot`, `status_effects`, `total_experience`, `exp_level`, `exp_progress`, `game_mode`, `statistics`, `is_flying`, `advancements`, `location`\n" +
                                                                                          "FROM `%source_players_table%`\n" +
                                                                                          "INNER JOIN `%source_data_table%`\n" +
                                                                                          "ON `%source_players_table%`.`id` = `%source_data_table%`.`player_id`\n" +
                                                                                          "WHERE `username` IS NOT NULL;\n").replaceAll(Pattern.quote("%source_players_table%"), sourcePlayersTable)
                            .replaceAll(Pattern.quote("%source_data_table%"), sourceDataTable))) {
                        try (final ResultSet resultSet = statement.executeQuery()) {
                            int playersMigrated = 0;
                            while (resultSet.next()) {
                                dataToMigrate.add(new LegacyData(
                                        new User(UUID.fromString(resultSet.getString("uuid")),
                                                resultSet.getString("username")),
                                        resultSet.getString("inventory"),
                                        resultSet.getString("ender_chest"),
                                        resultSet.getDouble("health"),
                                        resultSet.getDouble("max_health"),
                                        resultSet.getDouble("health_scale"),
                                        resultSet.getInt("hunger"),
                                        resultSet.getFloat("saturation"),
                                        resultSet.getFloat("saturation_exhaustion"),
                                        resultSet.getInt("selected_slot"),
                                        resultSet.getString("status_effects"),
                                        resultSet.getInt("total_experience"),
                                        resultSet.getInt("exp_level"),
                                        resultSet.getFloat("exp_progress"),
                                        resultSet.getString("game_mode"),
                                        resultSet.getString("statistics"),
                                        resultSet.getBoolean("is_flying"),
                                        resultSet.getString("advancements"),
                                        resultSet.getString("location")
                                ));
                                playersMigrated++;
                                if (playersMigrated % 50 == 0) {
                                    plugin.log(Level.INFO, "Downloaded legacy data for " + playersMigrated + " players...");
                                }
                            }
                        }
                    }
                }
                plugin.log(Level.INFO, "Completed download of " + dataToMigrate.size() + " entries from the legacy database!");
                plugin.log(Level.INFO, "Converting HuskSync 1.x data to the new user data format (this might take a while)...");

                final AtomicInteger playersConverted = new AtomicInteger();
                dataToMigrate.forEach(data -> data.toUserData(hslConverter, minecraftVersion).thenAccept(convertedData -> {
                    plugin.getDatabase().ensureUser(data.user()).thenRun(() ->
                            plugin.getDatabase().setUserData(data.user(), convertedData, DataSaveCause.LEGACY_MIGRATION)
                                    .exceptionally(exception -> {
                                        plugin.log(Level.SEVERE, "Failed to migrate legacy data for " + data.user().username + ": " + exception.getMessage());
                                        return null;
                                    })).join();

                    playersConverted.getAndIncrement();
                    if (playersConverted.get() % 50 == 0) {
                        plugin.log(Level.INFO, "Converted legacy data for " + playersConverted + " players...");
                    }
                }).join());
                plugin.log(Level.INFO, "Migration complete for " + dataToMigrate.size() + " users in " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds!");
                return true;
            } catch (Exception e) {
                plugin.log(Level.SEVERE, "Error while migrating legacy data: " + e.getMessage() + " - are your source database credentials correct?");
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
                case "players_table":
                    this.sourcePlayersTable = args[1];
                    b = true;
                    break;
                case "data_table":
                    this.sourceDataTable = args[1];
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
        return "legacy";
    }

    @NotNull
    @Override
    public String getName() {
        return "HuskSync v1.x --> v2.x Migrator";
    }

    @NotNull
    @Override
    public String getHelpMenu() {
        return ("=== HuskSync v1.x --> v2.x Migration Wizard =========\n" +
                "This will migrate all user data from HuskSync v1.x to\n" +
                "HuskSync v2.x's new format. To perform the migration,\n" +
                "please follow the steps below carefully.\n" +
                "\n" +
                "[!] Existing data in the database will be wiped. [!]\n" +
                "\n" +
                "STEP 1] Please ensure no players are on any servers.\n" +
                "\n" +
                "STEP 2] HuskSync will need to connect to the database\n" +
                "used to hold the existing, legacy HuskSync data.\n" +
                "If this is the same database as the one you are\n" +
                "currently using, you probably don't need to change\n" +
                "anything.\n" +
                "Please check that the credentials below are the\n" +
                "correct credentials of the source legacy HuskSync\n" +
                "database.\n" +
                "- host: %source_host%\n" +
                "- port: %source_port%\n" +
                "- username: %source_username%\n" +
                "- password: %source_password%\n" +
                "- database: %source_database%\n" +
                "- players_table: %source_players_table%\n" +
                "- data_table: %source_data_table%\n" +
                "If any of these are not correct, please correct them\n" +
                "using the command:\n" +
                "\"husksync migrate legacy set <parameter> <value>\"\n" +
                "(e.g.: \"husksync migrate legacy set host 1.2.3.4\")\n" +
                "\n" +
                "STEP 3] HuskSync will migrate data into the database\n" +
                "tables configures in the config.yml file of this\n" +
                "server. Please make sure you're happy with this\n" +
                "before proceeding.\n" +
                "\n" +
                "STEP 4] To start the migration, please run:\n" +
                "\"husksync migrate legacy start\"\n").replaceAll(Pattern.quote("%source_host%"), obfuscateDataString(sourceHost))
                .replaceAll(Pattern.quote("%source_port%"), Integer.toString(sourcePort))
                .replaceAll(Pattern.quote("%source_username%"), obfuscateDataString(sourceUsername))
                .replaceAll(Pattern.quote("%source_password%"), obfuscateDataString(sourcePassword))
                .replaceAll(Pattern.quote("%source_database%"), sourceDatabase)
                .replaceAll(Pattern.quote("%source_players_table%"), sourcePlayersTable)
                .replaceAll(Pattern.quote("%source_data_table%"), sourceDataTable);
    }

    private static final class LegacyData {
        @NotNull
        private final User user;
        @NotNull
        private final String serializedInventory;
        @NotNull
        private final String serializedEnderChest;
        private final double health;
        private final double maxHealth;
        private final double healthScale;
        private final int hunger;
        private final float saturation;
        private final float saturationExhaustion;
        private final int selectedSlot;
        @NotNull
        private final String serializedPotionEffects;
        private final int totalExp;
        private final int expLevel;
        private final float expProgress;
        @NotNull
        private final String gameMode;
        @NotNull
        private final String serializedStatistics;
        private final boolean isFlying;
        @NotNull
        private final String serializedAdvancements;
        @NotNull
        private final String serializedLocation;

        private LegacyData(@NotNull User user,
                           @NotNull String serializedInventory, @NotNull String serializedEnderChest,
                           double health, double maxHealth, double healthScale, int hunger, float saturation,
                           float saturationExhaustion, int selectedSlot, @NotNull String serializedPotionEffects,
                           int totalExp, int expLevel, float expProgress,
                           @NotNull String gameMode, @NotNull String serializedStatistics, boolean isFlying,
                           @NotNull String serializedAdvancements, @NotNull String serializedLocation) {
            this.user = user;
            this.serializedInventory = serializedInventory;
            this.serializedEnderChest = serializedEnderChest;
            this.health = health;
            this.maxHealth = maxHealth;
            this.healthScale = healthScale;
            this.hunger = hunger;
            this.saturation = saturation;
            this.saturationExhaustion = saturationExhaustion;
            this.selectedSlot = selectedSlot;
            this.serializedPotionEffects = serializedPotionEffects;
            this.totalExp = totalExp;
            this.expLevel = expLevel;
            this.expProgress = expProgress;
            this.gameMode = gameMode;
            this.serializedStatistics = serializedStatistics;
            this.isFlying = isFlying;
            this.serializedAdvancements = serializedAdvancements;
            this.serializedLocation = serializedLocation;
        }

            @NotNull
            public CompletableFuture<UserData> toUserData(@NotNull HSLConverter converter,
                                                          @NotNull String minecraftVersion) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        final DataSerializer.StatisticData legacyStatisticData = converter
                            .deserializeStatisticData(serializedStatistics);
                        final StatisticsData convertedStatisticData = new StatisticsData(
                            convertStatisticMap(legacyStatisticData.untypedStatisticValues()),
                            convertMaterialStatisticMap(legacyStatisticData.blockStatisticValues()),
                            convertMaterialStatisticMap(legacyStatisticData.itemStatisticValues()),
                            convertEntityStatisticMap(legacyStatisticData.entityStatisticValues()));

                        final List<AdvancementData> convertedAdvancements = converter
                            .deserializeAdvancementData(serializedAdvancements)
                            .stream().map(data -> new AdvancementData(data.key(), data.criteriaMap())).collect(Collectors.toList());

                        final DataSerializer.PlayerLocation legacyLocationData = converter
                            .deserializePlayerLocationData(serializedLocation);
                        final LocationData convertedLocationData = new LocationData(
                            legacyLocationData == null ? "world" : legacyLocationData.worldName(),
                            UUID.randomUUID(),
                            "NORMAL",
                            legacyLocationData == null ? 0d : legacyLocationData.x(),
                            legacyLocationData == null ? 64d : legacyLocationData.y(),
                            legacyLocationData == null ? 0d : legacyLocationData.z(),
                            legacyLocationData == null ? 90f : legacyLocationData.yaw(),
                            legacyLocationData == null ? 180f : legacyLocationData.pitch());

                        return UserData.builder(minecraftVersion)
                            .setStatus(new StatusData(health, maxHealth, healthScale, hunger, saturation,
                                saturationExhaustion, selectedSlot, totalExp, expLevel, expProgress, gameMode, isFlying))
                            .setInventory(new ItemData(serializedInventory))
                            .setEnderChest(new ItemData(serializedEnderChest))
                            .setPotionEffects(new PotionEffectData(serializedPotionEffects))
                            .setAdvancements(convertedAdvancements)
                            .setStatistics(convertedStatisticData)
                            .setLocation(convertedLocationData)
                            .build();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            private Map<String, Integer> convertStatisticMap(@NotNull HashMap<Statistic, Integer> rawMap) {
                final HashMap<String, Integer> convertedMap = new HashMap<>();
                for (Map.Entry<Statistic, Integer> entry : rawMap.entrySet()) {
                    convertedMap.put(entry.getKey().toString(), entry.getValue());
                }
                return convertedMap;
            }

            private Map<String, Map<String, Integer>> convertMaterialStatisticMap(@NotNull HashMap<Statistic, HashMap<Material, Integer>> rawMap) {
                final Map<String, Map<String, Integer>> convertedMap = new HashMap<>();
                for (Map.Entry<Statistic, HashMap<Material, Integer>> entry : rawMap.entrySet()) {
                    for (Map.Entry<Material, Integer> materialEntry : entry.getValue().entrySet()) {
                        convertedMap.computeIfAbsent(entry.getKey().toString(), k -> new HashMap<>())
                            .put(materialEntry.getKey().toString(), materialEntry.getValue());
                    }
                }
                return convertedMap;
            }

            private Map<String, Map<String, Integer>> convertEntityStatisticMap(@NotNull HashMap<Statistic, HashMap<EntityType, Integer>> rawMap) {
                final Map<String, Map<String, Integer>> convertedMap = new HashMap<>();
                for (Map.Entry<Statistic, HashMap<EntityType, Integer>> entry : rawMap.entrySet()) {
                    for (Map.Entry<EntityType, Integer> materialEntry : entry.getValue().entrySet()) {
                        convertedMap.computeIfAbsent(entry.getKey().toString(), k -> new HashMap<>())
                            .put(materialEntry.getKey().toString(), materialEntry.getValue());
                    }
                }
                return convertedMap;
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
        public String serializedEnderChest() {
            return serializedEnderChest;
        }

        public double health() {
            return health;
        }

        public double maxHealth() {
            return maxHealth;
        }

        public double healthScale() {
            return healthScale;
        }

        public int hunger() {
            return hunger;
        }

        public float saturation() {
            return saturation;
        }

        public float saturationExhaustion() {
            return saturationExhaustion;
        }

        public int selectedSlot() {
            return selectedSlot;
        }

        @NotNull
        public String serializedPotionEffects() {
            return serializedPotionEffects;
        }

        public int totalExp() {
            return totalExp;
        }

        public int expLevel() {
            return expLevel;
        }

        public float expProgress() {
            return expProgress;
        }

        @NotNull
        public String gameMode() {
            return gameMode;
        }

        @NotNull
        public String serializedStatistics() {
            return serializedStatistics;
        }

        public boolean isFlying() {
            return isFlying;
        }

        @NotNull
        public String serializedAdvancements() {
            return serializedAdvancements;
        }

        @NotNull
        public String serializedLocation() {
            return serializedLocation;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (LegacyData) obj;
            return Objects.equals(this.user, that.user) &&
                   Objects.equals(this.serializedInventory, that.serializedInventory) &&
                   Objects.equals(this.serializedEnderChest, that.serializedEnderChest) &&
                   Double.doubleToLongBits(this.health) == Double.doubleToLongBits(that.health) &&
                   Double.doubleToLongBits(this.maxHealth) == Double.doubleToLongBits(that.maxHealth) &&
                   Double.doubleToLongBits(this.healthScale) == Double.doubleToLongBits(that.healthScale) &&
                   this.hunger == that.hunger &&
                   Float.floatToIntBits(this.saturation) == Float.floatToIntBits(that.saturation) &&
                   Float.floatToIntBits(this.saturationExhaustion) == Float.floatToIntBits(that.saturationExhaustion) &&
                   this.selectedSlot == that.selectedSlot &&
                   Objects.equals(this.serializedPotionEffects, that.serializedPotionEffects) &&
                   this.totalExp == that.totalExp &&
                   this.expLevel == that.expLevel &&
                   Float.floatToIntBits(this.expProgress) == Float.floatToIntBits(that.expProgress) &&
                   Objects.equals(this.gameMode, that.gameMode) &&
                   Objects.equals(this.serializedStatistics, that.serializedStatistics) &&
                   this.isFlying == that.isFlying &&
                   Objects.equals(this.serializedAdvancements, that.serializedAdvancements) &&
                   Objects.equals(this.serializedLocation, that.serializedLocation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(user, serializedInventory, serializedEnderChest, health, maxHealth, healthScale, hunger, saturation, saturationExhaustion, selectedSlot, serializedPotionEffects, totalExp, expLevel, expProgress, gameMode, serializedStatistics, isFlying, serializedAdvancements, serializedLocation);
        }

        @Override
        public String toString() {
            return "LegacyData[" +
                   "user=" + user + ", " +
                   "serializedInventory=" + serializedInventory + ", " +
                   "serializedEnderChest=" + serializedEnderChest + ", " +
                   "health=" + health + ", " +
                   "maxHealth=" + maxHealth + ", " +
                   "healthScale=" + healthScale + ", " +
                   "hunger=" + hunger + ", " +
                   "saturation=" + saturation + ", " +
                   "saturationExhaustion=" + saturationExhaustion + ", " +
                   "selectedSlot=" + selectedSlot + ", " +
                   "serializedPotionEffects=" + serializedPotionEffects + ", " +
                   "totalExp=" + totalExp + ", " +
                   "expLevel=" + expLevel + ", " +
                   "expProgress=" + expProgress + ", " +
                   "gameMode=" + gameMode + ", " +
                   "serializedStatistics=" + serializedStatistics + ", " +
                   "isFlying=" + isFlying + ", " +
                   "serializedAdvancements=" + serializedAdvancements + ", " +
                   "serializedLocation=" + serializedLocation + ']';
        }


        }

}

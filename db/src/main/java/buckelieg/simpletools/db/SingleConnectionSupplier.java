package buckelieg.simpletools.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public final class SingleConnectionSupplier implements TrySupplier<Connection, SQLException> {

    public static final String URL = "jdbc.url";
    public static final String USER = "jdbc.user";
    public static final String PASSWORD = "jdbc.password";
    public static final String DRIVER_CLASS = "jdbc.driver";
    public static final String SQL = "jdbc.sql";


    private final AtomicReference<Connection> pool = new AtomicReference<>();
    private final AtomicReference<Boolean> driverInitialized = new AtomicReference<>(false);

    private final Properties settings;

    public SingleConnectionSupplier(Properties settings) {
        this.settings = Objects.requireNonNull(settings);
    }

    @Override
    public Connection get() throws SQLException {
        return pool.updateAndGet(c -> {
            Connection conn = c;
            try {
                if (conn == null) {
                    driverInitialized.updateAndGet(driverLoaded -> {
                        // despite of JDBC 4 spec states that JDBC-driver has to register itself within DriverManager
                        // we can face a case when driver is provided on cp with our jar. then we HAVE to load class manually
                        // in other cases just left jdbc.driver property empty in the properties file
                        if (!driverLoaded) {
                            Optional.ofNullable(settings.getProperty(DRIVER_CLASS)).ifPresent(driverClass -> {
                                try {
                                    Class.forName(driverClass);
                                } catch (ClassNotFoundException e) {
                                    // ignore possible invalid class' name
                                }
                            });
                        }
                        return true;
                    });
                    conn = getConnection();
                } else {
                    try {
                        if (conn.isClosed()) {
                            conn = getConnection();
                        }
                    } catch (AbstractMethodError ame) {
                        // ignore this driver's vendor specific error
                        try {
                            // TODO enumerate here all valid dummy statements for major RDBMS
                            // to be used as a defaults whenever jdbc.sql is not provided
                            // like SELECT 1 FROM DUAL for Oracle etc. (may be depending on driver's class name)?
                            conn.createStatement().execute(Optional.ofNullable(settings.getProperty(SQL)).orElse("SELECT 1"));
                        } catch (SQLException e) {
                            conn = getConnection();
                        }
                    }
                }
            } catch (SQLException e) {
                throw new SQLRuntimeException(e);
            }
            return conn;
        });
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                Optional.ofNullable(settings.getProperty(URL)).orElse(""),
                Optional.ofNullable(settings.getProperty(USER)).orElse(""),
                Optional.ofNullable(settings.getProperty(PASSWORD)).orElse("")
        );
    }
}

package io.vitess.jdbc;

import io.vitess.util.Constants;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessDriverTest {

    private static VitessDriver driver = new VitessDriver();

    String dbURL =
        "jdbc:vitess://localhost:9000/shipment/vt_shipment?tabletType=master&executeType=stream&userName"
            + "=user";

    @BeforeClass public static void setUp() {
        // load Vitess driver
        try {
            Class.forName("io.vitess.jdbc.VitessDriver");
        } catch (ClassNotFoundException e) {
            Assert.fail("Driver is not in the CLASSPATH -> " + e);
        }
    }

    @Test public void testConnect() {
        try {
            VitessConnection connection =
                (VitessConnection) DriverManager.getConnection(dbURL, new Properties());
            Assert.assertEquals(connection.getUrl().getUrl(), dbURL);
        } catch (SQLException e) {
            Assert.fail("SQLException Not Expected");
        }
    }

    @Test public void testAcceptsURL() {
        try {
            Assert.assertEquals(true, driver.acceptsURL(dbURL));
        } catch (SQLException e) {
            Assert.fail("SQLException Not Expected");
        }
    }

    @Test public void testAcceptsMalformedURL() {
        try {
            String url =
                "jdbc:MalfromdedUrl://localhost:9000/shipment/vt_shipment?tabletType=master";
            Assert.assertEquals(false, driver.acceptsURL(url));
        } catch (SQLException e) {
            Assert.fail("SQLException Not Expected");
        }
    }

    @Test public void testGetPropertyInfo() throws SQLException {
        // Used to ensure that we're properly adding the below URL-based properties at the beginning
        // of the full ConnectionProperties configuration
        DriverPropertyInfo[] underlying = ConnectionProperties.exposeAsDriverPropertyInfo(new Properties(), 0);


        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo(dbURL, null);
        Assert.assertEquals(underlying.length + 2, driverPropertyInfos.length);

        Assert.assertEquals(driverPropertyInfos[0].description, Constants.VITESS_HOST);
        Assert.assertEquals(driverPropertyInfos[0].required, true);
        Assert.assertEquals(driverPropertyInfos[0].name, Constants.Property.HOST);
        Assert.assertEquals(driverPropertyInfos[0].value, "localhost");

        Assert.assertEquals(driverPropertyInfos[1].description, Constants.VITESS_PORT);
        Assert.assertEquals(driverPropertyInfos[1].required, false);
        Assert.assertEquals(driverPropertyInfos[1].name, Constants.Property.PORT);
        Assert.assertEquals(driverPropertyInfos[1].value, "9000");

        // Validate the remainder of the driver properties match up with the underlying
        for (int i = 2; i < driverPropertyInfos.length; i++) {
            Assert.assertEquals(underlying[i - 2].description, driverPropertyInfos[i].description);
            Assert.assertEquals(underlying[i - 2].required, driverPropertyInfos[i].required);
            Assert.assertEquals(underlying[i - 2].name, driverPropertyInfos[i].name);
            Assert.assertEquals(underlying[i - 2].value, driverPropertyInfos[i].value);
        }
    }

    @Test public void testGetMajorVersion() {
        Assert.assertEquals(driver.getMajorVersion(), Constants.DRIVER_MAJOR_VERSION);
    }

    @Test public void testGetMinorVersion() {
        Assert.assertEquals(driver.getMinorVersion(), Constants.DRIVER_MINOR_VERSION);
    }

    @Test public void testJdbcCompliant() {
        Assert.assertEquals(false, driver.jdbcCompliant());
    }

}

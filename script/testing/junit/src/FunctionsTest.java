/**
 * Insert statement tests.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Types.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class FunctionsTest extends TestUtility {
    private Connection conn;
    private ResultSet rs;
    private String S_SQL = "SELECT * FROM data;";

    private static final String SQL_DROP_TABLE =
            "DROP TABLE IF EXISTS data;";

    private static final String SQL_CREATE_TABLE =
            "CREATE TABLE data (" +
                    "int_val INT, " +
                    "double_val DECIMAL," +
                    "small_double_val DECIMAL," +
                    "str_i_val VARCHAR(32)," + // Integer as string
                    "str_a_val VARCHAR(32)," + // Alpha string
                    "str_b_val VARCHAR(32)," + // Beta string
//                     "bool_val BOOL," +
                    "is_null INT)";

    /**
     * Initialize the database and table for testing
     */
    private void initDatabase() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
        stmt.execute(SQL_CREATE_TABLE);

        String sql = "INSERT INTO data (" +
                     "int_val, double_val, small_double_val, str_i_val, str_a_val, str_b_val, " +
//                      "bool_val, " + 
                     "is_null " +
                     ") VALUES (?, ?, ?, ?, ?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);

        // Non-Null Values
        int idx = 1;
        pstmt.setInt(idx++, 123);
        pstmt.setDouble(idx++, 12.34);
        pstmt.setDouble(idx++, 0.5);
        pstmt.setString(idx++, "123456");
        pstmt.setString(idx++, "AbCdEf");
        pstmt.setString(idx++, "  AbCdEf ");
//         pstmt.setBoolean(idx++, true);
        pstmt.setInt(idx++, 0);
//         pstmt.setBoolean(idx++, false);
        pstmt.addBatch();
        
        // Null Values
        idx = 1;
        pstmt.setNull(idx++, java.sql.Types.INTEGER);
        pstmt.setNull(idx++, java.sql.Types.DOUBLE);
        pstmt.setNull(idx++, java.sql.Types.DOUBLE);
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);
//         pstmt.setNull(idx++, java.sql.Types.BOOLEAN);
        pstmt.setInt(idx++, 1);
//         pstmt.setBoolean(idx++, true);
        pstmt.addBatch();
        
        pstmt.executeBatch();
    }

    /**
     * Setup for each test, execute before each test
     * reconnect and setup default table
     */
    @Before
    public void setup() throws SQLException {
        conn = makeDefaultConnection();
        conn.setAutoCommit(true);
        initDatabase();
    }

    /**
     * Cleanup for each test, execute after each test
     * drop the default table
     */
    @After
    public void teardown() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(SQL_DROP_TABLE);
    }

    /* --------------------------------------------
     * UDF statement tests
     * ---------------------------------------------
     */

    private void checkDoubleFunc(String func_name, String col_name, boolean is_null, Double expected) throws SQLException {
        String sql = String.format("SELECT %s(%s) AS result FROM data WHERE is_null = %s",
                                   func_name, col_name, (is_null ? 1 : 0));
                                   
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean exists = rs.next();
        assert(exists);
        if (is_null) {
            checkDoubleRow(rs, new String[]{"result"}, new Double[]{null});
        } else {
            checkDoubleRow(rs, new String[]{"result"}, new Double[]{expected});
        }
        assertNoMoreRows(rs);
    }
    
    private void checkStringFunc(String func_name, String col_name, boolean is_null, String expected) throws SQLException {
        String sql = String.format("SELECT %s(%s) AS result FROM data WHERE is_null = %s",
                                   func_name, col_name, (is_null ? 1 : 0));
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean exists = rs.next();
        assert(exists);
        if (is_null) {
            checkStringRow(rs, new String[]{"result"}, new String[]{null});
        } else {
            checkStringRow(rs, new String[]{"result"}, new String[]{expected});
        }
        assertNoMoreRows(rs);
    }

    private void checkStringFuncTrim(String func_name, String col_name, String trim_str, boolean is_null, String expected) throws SQLException {
        String sql = String.format("SELECT %s(%s, %s) AS result FROM data WHERE is_null = %s",
                                   func_name, col_name, trim_str, (is_null ? 1 : 0));
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean exists = rs.next();
        assert(exists);
        if (is_null) {
            checkStringRow(rs, new String[]{"result"}, new String[]{null});
        } else {
            checkStringRow(rs, new String[]{"result"}, new String[]{expected});
        }
        assertNoMoreRows(rs);
    }

    private void checkLeftRightFunc(String func_name, String col_name, int length, boolean is_null, String expected) throws SQLException {
        String sql = String.format("SELECT %s(%s, %d) AS result FROM data WHERE is_null = %s",
                                   func_name, col_name, length, (is_null ? 1 : 0));

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        boolean exists = rs.next();
        assert(exists);
        if (is_null) {
            checkStringRow(rs, new String[]{"result"}, new String[]{null});
        } else {
            checkStringRow(rs, new String[]{"result"}, new String[]{expected});
        }
        assertNoMoreRows(rs);
    }
    /**
     * Tests usage of trig udf functions
     * #744 test
     */
    @Test
    public void testCos() throws SQLException {
        checkDoubleFunc("cos", "double_val", false, 0.974487);
        checkDoubleFunc("cos", "double_val", true, null);
    }
    @Test
    public void testSin() throws SQLException {
        checkDoubleFunc("sin", "double_val", false, -0.224442);
        checkDoubleFunc("sin", "double_val", true, null);
    }
    @Test
    public void testTan() throws SQLException {
        checkDoubleFunc("tan", "double_val", false, -0.230318);
        checkDoubleFunc("tan", "double_val", true, null);
    }
    @Test
    public void testAbs() throws SQLException {
        checkDoubleFunc("abs", "double_val", false, 12.34);
        checkDoubleFunc("abs", "double_val", true, null);
    }
  
    @Test
    public void testACos() throws SQLException {
        checkDoubleFunc("acos", "small_double_val", false, 1.047198);
        checkDoubleFunc("acos", "small_double_val", true, null);
    }
    @Test
    public void testASin() throws SQLException {
        checkDoubleFunc("asin", "small_double_val", false, 0.523599);
        checkDoubleFunc("asin", "small_double_val", true, null);
    }
    @Test
    public void testATan() throws SQLException {
        checkDoubleFunc("atan", "small_double_val", false, 0.463648);
        checkDoubleFunc("atan", "small_double_val", true, null);
    }
    
    /**
     * String Functions
     */
    @Test
    public void testLower() throws SQLException {
        checkStringFunc("lower", "str_a_val", false, "abcdef");
        checkStringFunc("lower", "str_a_val", true, null);
    }


    @Test
    public void testTrim() throws SQLException {
        checkStringFunc("trim", "str_b_val", false, "AbCdEf");
        checkStringFuncTrim("trim", "str_b_val", "' f'", false, "AbCdE");
        checkStringFunc("trim", "str_b_val", true, null);
    }

    @Test
    public void testUpper() throws SQLException {
        checkStringFunc("upper", "str_a_val", false, "ABCDEF");
        checkStringFunc("upper", "str_a_val", true, null);
    }

    @Test
    public void testReverse() throws SQLException {
        checkStringFunc("reverse", "str_a_val", false, "fEdCbA");
        checkStringFunc("reverse", "str_a_val", true, null);
    }

    @Test
    public void testLeft() throws SQLException {
        checkLeftRightFunc("left", "str_a_val", 3, false, "AbC");
        checkLeftRightFunc("left", "str_a_val", 3, true, null);
        checkLeftRightFunc("left", "str_a_val", -2, false, "AbCd");
        checkLeftRightFunc("left", "str_a_val", 10, false, "AbCdEf");
        checkLeftRightFunc("left", "str_a_val", -10, false, "");
    }

    @Test
    public void testRight() throws SQLException {
        checkLeftRightFunc("right", "str_a_val", 3, false, "dEf");
        checkLeftRightFunc("right", "str_a_val", 3, true, null);
        checkLeftRightFunc("right", "str_a_val", -2, false, "CdEf");
        checkLeftRightFunc("right", "str_a_val", 10, false, "AbCdEf");
        checkLeftRightFunc("right", "str_a_val", -10, false, "");
    }

    @Test
    public void testRepeat() throws SQLException {
        checkLeftRightFunc("repeat", "str_a_val", -1, false, "");
        checkLeftRightFunc("repeat", "str_a_val", 0, false, "");
        checkLeftRightFunc("repeat", "str_a_val", 1, false, "AbCdEf");
        checkLeftRightFunc("repeat", "str_a_val", 2, false, "AbCdEfAbCdEf");
        checkLeftRightFunc("repeat", "str_a_val", 3, false, "AbCdEfAbCdEfAbCdEf");
        checkLeftRightFunc("repeat", "str_a_val", 4, false, "AbCdEfAbCdEfAbCdEfAbCdEf");
        checkLeftRightFunc("repeat", "str_a_val", 4, true, null);
    }

}

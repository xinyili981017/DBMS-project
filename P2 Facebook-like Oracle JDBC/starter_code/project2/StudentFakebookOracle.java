package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            //step 1 : longest First_Name
            FirstNameInfo info = new FirstNameInfo();
            ResultSet rst1 = stmt.executeQuery(
                    "SELECT DISTINCT First_Name " +
                    "FROM " + UsersTable + " " +
                    "WHERE LENGTH(First_Name) = " + "(SELECT MAX(LENGTH(First_Name)) " + "FROM " + UsersTable + ")" +
                    "ORDER BY First_Name");
            while(rst1.next()){
                info.addLongName(rst1.getString(1));
            }
            //Step 2 : The first name(s) with the fewest letters
            rst1 = stmt.executeQuery(
                    "SELECT DISTINCT First_Name " +
                    "FROM " +  UsersTable + " " +
                    "WHERE LENGTH(First_Name) = " + "(SELECT MIN(LENGTH(First_Name)) " + "FROM " + UsersTable + ")" +
                    "ORDER BY First_Name"
            );
            while(rst1.next()){
                info.addShortName(rst1.getString(1));
            }
            //(C) The first name held by the most users
            rst1 = stmt.executeQuery(
                    "SELECT COUNT(*), First_Name " +
                    "FROM " + UsersTable + " " +
                    "GROUP BY First_Name " +
                    "HAVING COUNT(*) = " +
                    "(SELECT MAX(COUNT(*)) " +
                    "FROM " + UsersTable + " " +
                    "GROUP BY First_Name) " +
                    "ORDER BY COUNT(*) DESC"
            );
            while(rst1.next()){
                info.setCommonNameCount(rst1.getLong(1));
                info.addCommonName(rst1.getString(2));
            }
            rst1.close();
            stmt.close();

            return info;
//            return new FirstNameInfo();                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst2 = stmt.executeQuery(
                    "SELECT USER_ID,First_Name,Last_Name " +
                    "FROM " + UsersTable + " " +
                    "WHERE USER_ID not in " +
                            "(SELECT USER1_ID " + "FROM " + FriendsTable + ")" +
                    "AND USER_ID not in " +
                            "(SELECT USER2_ID " + "FROM " + FriendsTable + ")" +
                    "ORDER BY USER_ID"
            );
            while(rst2.next()) {
                results.add(new UserInfo(rst2.getLong(1), rst2.getString(2), rst2.getString(3)));
            }
            rst2.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
            ResultSet rst3 = stmt.executeQuery(
            "SELECT PU.USER_ID,First_Name,Last_Name " +
            "FROM " + CurrentCitiesTable + " PUCC, " + HometownCitiesTable + " PUHC, " + UsersTable + " PU " +
            "WHERE PU.USER_ID = PUCC.USER_ID " +
            "AND PUCC.USER_ID = PUHC.USER_ID " +
            "AND PUCC.CURRENT_CITY_ID <> PUHC.HOMETOWN_CITY_ID " +
            "ORDER BY PU.USER_ID");
            while(rst3.next()) {
                results.add(new UserInfo(rst3.getLong(1), rst3.getString(2), rst3.getString(3)));
            }
            rst3.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }
    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
            //step 1: Find the IDs, links, and IDs and names of the containing album of the top
            // <num> photos with the most tagged users
            ResultSet rst4 = stmt.executeQuery(
                    "SELECT TAG_PHOTO_ID, ALBUM_ID, PHOTO_LINK, ALBUM_NAME " +
                    "FROM " +
                    "(SELECT TAG_PHOTO_ID, PHOTO_LINK, A.ALBUM_ID,ALBUM_NAME,COUNT(*) " +
                    "FROM " + TagsTable + " T, " + AlbumsTable + " A, " + PhotosTable + " P " +
                    "WHERE T.TAG_PHOTO_ID = P.PHOTO_ID " +
                    "AND P.ALBUM_ID = A.ALBUM_ID " +
                    "GROUP BY TAG_PHOTO_ID, PHOTO_LINK, A.ALBUM_ID, ALBUM_NAME " +
                    "ORDER BY COUNT(*) DESC, TAG_PHOTO_ID ASC) " +
                    "WHERE ROWNUM <=" + Integer.toString(num));
            long pid;
            Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            while(rst4.next()) {
                pid = rst4.getLong(1);
                PhotoInfo p = new PhotoInfo(pid,rst4.getLong(2),rst4.getString(3),rst4.getString(4));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                ResultSet rst4a = stmt2.executeQuery(
                        "SELECT DISTINCT TAG_SUBJECT_ID,First_Name,Last_Name " +
                        "FROM " + TagsTable + " T, " + UsersTable + " U " +
                        "WHERE T.TAG_SUBJECT_ID = U.USER_ID " +
                        "AND T.TAG_PHOTO_ID = " + pid +
                        "ORDER BY TAG_SUBJECT_ID ASC");
                while(rst4a.next()){
                    tp.addTaggedUser(new UserInfo(rst4a.getLong(1),rst4a.getString(2),rst4a.getString(3)));
                }
                results.add(tp);
                rst4a.close();
            }
//            rst4a.close();
            rst4.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }
    
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
            ResultSet rst5 = stmt.executeQuery(
                    "SELECT U1,U2,U3,U4,U5,U6,U7,U8 FROM " +
                    "(SELECT U1.USER_ID AS U1,U1.First_Name AS U2,U1.Last_Name AS U3,U1.YEAR_OF_BIRTH AS U4,U2.USER_ID AS U5,U2.First_Name AS U6,U2.Last_Name AS U7,U2.YEAR_OF_BIRTH AS U8 " +
                    "FROM " + UsersTable + " U1," + UsersTable + " U2," +
                    "(SELECT T1.TAG_SUBJECT_ID AS tid1, T2.TAG_SUBJECT_ID AS tid2,COUNT(*) AS num_photos " +
                    "FROM " + TagsTable + " T1," + TagsTable + " T2 " +
                    "WHERE T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " +
                    "AND T1.TAG_SUBJECT_ID < T2.TAG_SUBJECT_ID " +
                    "GROUP BY T1.TAG_SUBJECT_ID, T2.TAG_SUBJECT_ID)X " +
                    "WHERE U1.USER_ID < U2.USER_ID AND U1.GENDER = U2.GENDER " +
                    "AND U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH >= " + (-1) * yearDiff + " " +
                    "AND U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH <= " + yearDiff + " " +
                    "AND U1.USER_ID = tid1 AND U2.USER_ID = tid2 " +
                    "AND (U1.USER_ID, U2.USER_ID) NOT IN " +
                    "(SELECT USER1_ID, USER2_ID "  + "FROM " + FriendsTable + ") " +
                    "ORDER BY num_photos DESC, U1.USER_ID, U2.USER_ID) " +
                    "WHERE ROWNUM <= " + num
            );
            long uid1;
            long uid2;
            Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            while(rst5.next()){
                uid1 = rst5.getLong(1);
                uid2 = rst5.getLong(5);
                UserInfo u1 = new UserInfo(rst5.getLong(1),rst5.getString(2),rst5.getString(3));
                UserInfo u2 = new UserInfo(rst5.getLong(5),rst5.getString(6),rst5.getString(7));
                MatchPair mp = new MatchPair(u1, rst5.getLong(4), u2, rst5.getLong(8));
                ResultSet rst5a = stmt2.executeQuery(
                        "SELECT P.PHOTO_ID,A.ALBUM_ID,PHOTO_LINK,ALBUM_NAME " +
                        "FROM " + TagsTable + " T1, " + AlbumsTable + " A, " + PhotosTable + " P, " + TagsTable + " T2 " +
                        "WHERE T1.TAG_SUBJECT_ID = " + uid1 + " " +
                        "AND T2.TAG_SUBJECT_ID = " + uid2 + " " +
                        "AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " +
                        "AND P.ALBUM_ID = A.ALBUM_ID " +
                        "AND P.PHOTO_ID = T1.TAG_PHOTO_ID " +
                        "ORDER BY P.PHOTO_ID ASC"
                );
                while(rst5a.next()){
                    PhotoInfo p = new PhotoInfo(rst5a.getLong(1), rst5a.getLong(2), rst5a.getString(3), rst5a.getString(4));
                    mp.addSharedPhoto(p);
                }
            results.add(mp);
            }

        }

        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
            stmt.executeUpdate(
                    "CREATE OR REPLACE VIEW TEMP_VIEW AS " +
                            "SELECT USER1_ID, USER2_ID " +
                            "FROM " + FriendsTable + " " +
                            "UNION " +
                            "SELECT USER2_ID, USER1_ID " +
                            "FROM " + FriendsTable + " ");

            ResultSet rst6 = stmt.executeQuery(
                    "SELECT U1,U12,U13,U2,U22,U23,num FROM " +
                    "(SELECT X.USER1 AS U1, U1.First_Name AS U12, U1.Last_Name as U13, X.USER2 AS U2,U2.First_Name AS U22,U2.Last_Name AS U23,COUNT(*) AS num " +
                    "FROM " + UsersTable + " U1, " + UsersTable + " U2, " +
                    "(SELECT A.USER1_ID AS USER1, A.USER2_ID AS MUTUAL, B.USER2_ID AS USER2 " +
                    "FROM TEMP_VIEW A, TEMP_VIEW B " +
                    "WHERE A.USER2_ID = B.USER1_ID " +
                    "AND A.USER1_ID < B.USER2_ID " +
                    "AND (A.USER1_ID, B.USER2_ID) NOT IN (SELECT USER1_ID, USER2_ID FROM " + FriendsTable +" )) X " +
                    "WHERE U1.USER_ID = X.USER1 " +
                    "AND U2.USER_ID = X.USER2 " +
                    "GROUP BY X.USER1,U1.First_Name,U1.Last_Name, X.USER2,U2.First_Name,U2.Last_Name " +
                    "ORDER BY COUNT(*) DESC, U1,U2) " +
                    "WHERE ROWNUM <= " + num);

            long u1id;
            long u2id;
            Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            while(rst6.next()) {
                u1id = rst6.getLong(1);
                u2id = rst6.getLong(4);
                UserInfo u1 = new UserInfo(rst6.getLong(1), rst6.getString(2), rst6.getString(3));
                UserInfo u2 = new UserInfo(rst6.getLong(4), rst6.getString(5), rst6.getString(6));
                UsersPair up = new UsersPair(u1, u2);
                ResultSet rst6a = stmt2.executeQuery(
                        "SELECT Y.MU_ID,U.First_Name,U.Last_Name " +
                        "FROM " + UsersTable + " U, " +
                        "(SELECT X.MUTUAL AS MU_ID FROM " +
                                "(SELECT A.USER1_ID AS USER1, A.USER2_ID AS MUTUAL, B.USER2_ID AS USER2 " +
                                "FROM TEMP_VIEW A, TEMP_VIEW B " +
                                "WHERE A.USER2_ID = B.USER1_ID " +
                                "AND A.USER1_ID < B.USER2_ID " +
                                "AND (A.USER1_ID, B.USER2_ID) NOT IN (SELECT USER1_ID, USER2_ID FROM " + FriendsTable + "))X " +
                                "WHERE X.USER1 = " + u1id + " " +
                                "AND X.USER2 = " + u2id + " " +
                                "ORDER BY X.MUTUAL)Y " +
                                "WHERE Y.MU_ID = U.USER_ID");
                while (rst6a.next()) {
                    UserInfo u3 = new UserInfo(rst6a.getLong(1), rst6a.getString(2), rst6a.getString(3));
                    up.addSharedFriend(u3);
                }
                results.add(up);
            }
            stmt.executeUpdate("DROP VIEW TEMP");
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            // step A: Find the name of the state or states in which the most events are held
            ResultSet rst7 = stmt.executeQuery(
                    "SELECT STATE_NAME, COUNT(*) " +
                    "FROM " + EventsTable + " E," + CitiesTable + " C " +
                    "WHERE E.EVENT_CITY_ID = C.CITY_ID " +
                    "GROUP BY STATE_NAME " +
                    "HAVING COUNT(*) = " +
                    "(SELECT MAX(COUNT(*)) " +
                    "FROM " + EventsTable + " E," + CitiesTable + " C " +
                    "WHERE E.EVENT_CITY_ID = C.CITY_ID " +
                    "GROUP BY STATE_NAME) " +
                    "ORDER BY STATE_NAME ASC");
            rst7.beforeFirst();
            long count = 0;
            while(rst7.next()){
                count += rst7.getLong(2);
            }
            EventStateInfo info = new EventStateInfo(count);
            rst7.beforeFirst();
            while(rst7.next()){
                info.addState(rst7.getString(1));
            }
            rst7.close();
            stmt.close();
            return info;

//            return new EventStateInfo(-1);                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }
    
    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            ResultSet rst8 = stmt.executeQuery(
                    "SELECT MAX(YEAR_OF_BIRTH),MIN(YEAR_OF_BIRTH) " +
                    "FROM " + UsersTable + ", " +
                    "(SELECT USER1_ID,USER2_ID " +
                    "FROM " + FriendsTable + " " +
                    "WHERE USER1_ID = " + userID + " OR USER2_ID = "+ userID + ") " +
                    "WHERE USER_ID = USER1_ID AND USER1_ID <>" + userID + " " +
                    "OR (USER_ID = USER2_ID AND USER2_ID <>" + userID + ") "
            );
            rst8.first();
            long max_num;
            long min_num;
            max_num = rst8.getLong(1);
            min_num = rst8.getLong(2);
            rst8 = stmt.executeQuery(
                    "SELECT * FROM " +
                    "(SELECT USER_ID,First_Name,Last_Name " +
                    "FROM " + UsersTable + " " +
                    "WHERE YEAR_OF_BIRTH = " + min_num + " "+
                    "ORDER BY USER_ID DESC) " +
                    "WHERE ROWNUM <= 1"
            );
            rst8.first();
            UserInfo old = new UserInfo(rst8.getLong(1), rst8.getString(2), rst8.getString(3));
            rst8 = stmt.executeQuery(
                    "SELECT * FROM " +
                    "(SELECT USER_ID,First_Name,Last_Name " +
                    "FROM " + UsersTable + " " +
                    "WHERE YEAR_OF_BIRTH = " + max_num + " "+
                    "ORDER BY USER_ID DESC) " +
                    "WHERE ROWNUM <= 1"
            );
            rst8.first();
            UserInfo young = new UserInfo(rst8.getLong(1), rst8.getString(2), rst8.getString(3));
            return new AgeInfo(old, young);

//            return new AgeInfo(new UserInfo(-1, "UNWRITTEN", "UNWRITTEN"), new UserInfo(-1, "UNWRITTEN", "UNWRITTEN"));                // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
            ResultSet rst9 = stmt.executeQuery(
                   " SELECT U1.USER_ID,U1.First_Name,U1.Last_Name,U2.USER_ID,U2.First_Name,U2.Last_Name " +
                    "FROM " + UsersTable + " U1 " +
                    "INNER JOIN " + UsersTable + " U2 ON U1.LAST_NAME = U2.LAST_NAME " +
                    "INNER JOIN " + HometownCitiesTable + " HC1 ON HC1.USER_ID = U1.USER_ID " +
                    "INNER JOIN " + HometownCitiesTable + " HC2 ON HC2.USER_ID = U2.USER_ID " +
                    "WHERE U1.USER_ID < U2.USER_ID " +
                    "AND HC1.HOMETOWN_CITY_ID = HC2.HOMETOWN_CITY_ID " +
                    "AND ABS(U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10 " +
                    "AND (U1.USER_ID, U2.USER_ID) IN (SELECT USER1_ID, USER2_ID FROM " + FriendsTable + ") " +
                    "ORDER BY U1.USER_ID, U2.USER_ID"
            );
            while(rst9.next()){
                UserInfo u1 = new UserInfo(rst9.getLong(1),rst9.getString(2),rst9.getString(3));
                UserInfo u2 = new UserInfo(rst9.getLong(4),rst9.getString(5),rst9.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}

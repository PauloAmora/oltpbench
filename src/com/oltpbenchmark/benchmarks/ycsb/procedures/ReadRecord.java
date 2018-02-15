/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.ycsb.YCSBConstants;

public class ReadRecord extends Procedure {

	public SQLStmt[] readStmts = { new SQLStmt("SELECT field0 FROM USERTABLE WHERE YCSB_KEY=?"),
			new SQLStmt("SELECT field2, field4 FROM USERTABLE WHERE YCSB_KEY=?"),
			new SQLStmt("SELECT field1, field2, field3 FROM USERTABLE WHERE YCSB_KEY=?"),
			new SQLStmt("SELECT field1, field2, field6, field7 FROM USERTABLE WHERE YCSB_KEY=?"),
			new SQLStmt("SELECT field0, field1, field5, field8, field9 FROM USERTABLE WHERE YCSB_KEY=?") };

	Random r = new Random();

	// FIXME: The value in ysqb is a byteiterator
	public void run(Connection conn, int keyname, String results[]) throws SQLException {

		int idx = r.nextInt(5);
		PreparedStatement stmt = this.getPreparedStatement(conn, readStmts[idx]);
		stmt.setInt(1, keyname);
		ResultSet r = stmt.executeQuery();
		while (r.next()) {
			for (int i = 0; i < idx; i++)
				results[i] = r.getString(i + 1);
		} // WHILE
		r.close();
	}

}

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
import java.sql.SQLException;
import java.util.Random;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class DeleteRecord extends Procedure{
    public final SQLStmt retrieveStmt = new SQLStmt(
        "select retrieve_records(?)"
    );
    
    public final SQLStmt deleteStmt = new SQLStmt(
            "select delete_records(?)"
        );
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int keyname) throws SQLException {
    	Random r = new Random();
    	if(r.nextDouble() - 0.8 >= 0.0) {
    		 PreparedStatement stmt = this.getPreparedStatement(conn, deleteStmt);
    	        stmt.setInt(1, keyname);          
    	        stmt.execute();
    	} else {
    		 PreparedStatement stmt = this.getPreparedStatement(conn, retrieveStmt);
 	        stmt.setInt(1, keyname);          
 	        stmt.execute();
    	}
    }

}

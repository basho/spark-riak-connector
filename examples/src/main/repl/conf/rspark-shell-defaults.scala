/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
println("Welcome again, this time with taste of\n" +
  "     ___  _      __       __    ____              __  \n" +
  "    / _ \\(_)__ _/ /__  __/ /_  / __/__  ___ _____/ /__\n" +
  "   / , _/ / _ `/  '_/ /_  __/ _\\ \\/ _ \\/ _ `/ __/  '_/\n" +
  "  /_/|_/_/\\_,_/_/\\_\\   /_/   /___/ .__/\\_,_/_/ /_/\\_\\ \n" +
  "                                /_/                   \n")

print("Spark With Riak Connector (Riak " + sc.getConf.get("spark.riak.connection.host") + ")\n" +
  "(Riak address might be changed by editing param in rspark)\n\n" +

  "To load & run example (it requires to stop the existing SparkContext)\n" +
  "    :load examples/<example-name>.scala\n" +
  "    <example-name>.main(Array())\n\n")

import com.basho.riak.spark._

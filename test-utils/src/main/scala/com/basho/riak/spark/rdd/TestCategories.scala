package com.basho.riak.spark.rdd

/**
 * @author zkhadikova
 */

trait IntegrationTests

trait RegressionTests extends IntegrationTests

trait RiakKVTests extends IntegrationTests

trait RiakTSTests extends IntegrationTests

trait RiakCommonTests extends RiakKVTests with RiakTSTests
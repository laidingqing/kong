package com.kong.eos.sdk.pipeline.aggregation.operator

trait Associative {

    self: Operator =>
    def associativity(values: Iterable[(String, Option[Any])]): Option[Any]
}

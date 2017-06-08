/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplowanalytics
package snowplow.enrich.common
package adapters
package registry

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client.{Resolver, SchemaKey}

// This project
import loaders.CollectorPayload

/**
 * Transforms a collector payload which conforms to a known version of the Google Analytics
 * protocol into raw events.
 */
object MeasurementProtocolAdapter extends Adapter {

  // for failure messages
  private val vendorName = "MeasurementProtocol"
  private val gaVendor = "com.google.analytics"
  private val vendor = s"$gaVendor.measurement-protocol"
  private val protocolVersion = "v1"
  private val protocol = s"$vendor-$protocolVersion"
  private val format = "jsonschema"
  private val schemaVersion = "1-0-0"

  case class MPData(schemaUri: String, translationTable: Map[String, String])

  private val unstructEventData = Map(
    "pageview" -> MPData(
      SchemaKey(vendor, "page_view", format, schemaVersion).toSchemaUri,
      Map(
        "dl" -> "documentLocationURL",
        "dh" -> "documentHostName",
        "dp" -> "documentPath",
        "dt" -> "documentTitle"
      )
    ),
    "screenview" -> MPData(
      SchemaKey(vendor, "screen_view", format, schemaVersion).toSchemaUri, Map("cd" -> "name")),
    "event" -> MPData(
      SchemaKey(vendor, "event", format, schemaVersion).toSchemaUri,
      Map(
        "ec" -> "category",
        "ea" -> "action",
        "el" -> "label",
        "ev" -> "value"
      )
    ),
    "transaction" -> MPData(
      SchemaKey(vendor, "transaction", format, schemaVersion).toSchemaUri,
      Map(
        "ti"  -> "id",
        "ta"  -> "affiliation",
        "tr"  -> "revenue",
        "ts"  -> "shipping",
        "tt"  -> "tax",
        "tcc" -> "couponCode",
        "cu"  -> "currencyCode"
      )
    ),
    "item" -> MPData(
      SchemaKey(vendor, "item", format, schemaVersion).toSchemaUri,
      Map(
        "ti" -> "id",
        "in" -> "name",
        "ip" -> "price",
        "iq" -> "quantity",
        "ic" -> "code",
        "iv" -> "category",
        "cu" -> "currencyCode"
      )
    ),
    "social" -> MPData(
      SchemaKey(vendor, "social", format, schemaVersion).toSchemaUri,
      Map(
        "sn" -> "network",
        "sa" -> "action",
        "st" -> "actionTarget"
      )
    ),
    "exception" -> MPData(
      SchemaKey(vendor, "exception", format, schemaVersion).toSchemaUri,
      Map(
        "exd" -> "description",
        "exf" -> "isFatal"
      )
    ),
    "timing" -> MPData(
      SchemaKey(vendor, "timing", format, schemaVersion).toSchemaUri,
      Map(
        "utc" -> "userTimingCategory",
        "utv" -> "userTimingVariableName",
        "utt" -> "userTimingTime",
        "utl" -> "userTimingLabel",
        "plt" -> "pageLoadTime",
        "dns" -> "dnsTime",
        "pdt" -> "pageDownloadTime",
        "rrt" -> "redirectResponseTime",
        "tcp" -> "tcpConnectTime",
        "srt" -> "serverResponseTime",
        "dit" -> "domInteractiveTime",
        "clt" -> "contentLoadTime"
      )
    )
  )

  private val contextData = List(
    MPData(SchemaKey(gaVendor, "undocumented", format, schemaVersion).toSchemaUri,
      List("a", "jid", "gjid").map(e => e -> e).toMap),
    MPData(SchemaKey(gaVendor, "private", format, schemaVersion).toSchemaUri,
      List("_v", "_s", "_u", "_gid", "_r").map(e => e -> e.tail).toMap),
    MPData(SchemaKey(vendor, "general", format, schemaVersion).toSchemaUri,
      Map(
        "v"   -> "protocolVersion",
        "tid" -> "trackingId",
        "aip" -> "anonymizeIp",
        "ds"  -> "dataSource",
        "qt"  -> "queueTime",
        "z"   -> "cacheBuster"
      )
    ),
    MPData(SchemaKey(vendor, "user", format, schemaVersion).toSchemaUri,
      Map("cid" -> "clientId", "uid" -> "userId")),
    MPData(SchemaKey(vendor, "session", format, schemaVersion).toSchemaUri,
      Map(
        "sc"    -> "sessionControl",
        "uip"   -> "ipOverride",
        "ua"    -> "userAgentOverride",
        "geoid" -> "geographicalOverride"
      )
    ),
    MPData(SchemaKey(vendor, "traffic_source", format, schemaVersion).toSchemaUri,
      Map(
        "dr"    -> "documentReferrer",
        "cn"    -> "campaignName",
        "cs"    -> "campaignSource",
        "cm"    -> "campaignMedium",
        "ck"    -> "campaignKeyword",
        "cc"    -> "campaignContent",
        "ci"    -> "campaignId",
        "gclid" -> "googleAdwordsId",
        "dclid" -> "googleDisplayAdsId"
      )
    ),
    MPData(SchemaKey(vendor, "system_info", format, schemaVersion).toSchemaUri,
      Map(
        "sr" -> "screenResolution",
        "vp" -> "viewportSize",
        "de" -> "documentEncoding",
        "sd" -> "screenColors",
        "ul" -> "userLanguage",
        "je" -> "javaEnabled",
        "fl" -> "flashVersion"
      )
    ),
    MPData(SchemaKey(vendor, "link", format, schemaVersion).toSchemaUri, Map("linkid" -> "id")),
    MPData(SchemaKey(vendor, "app", format, schemaVersion).toSchemaUri,
      Map(
        "an"   -> "name",
        "aid"  -> "id",
        "av"   -> "version",
        "aiid" -> "installerId"
      )
    ),
    MPData(SchemaKey(vendor, "product_action", format, schemaVersion).toSchemaUri,
      Map(
        "pa"  -> "productAction",
        "pal" -> "productActionList",
        "cos" -> "checkoutStep",
        "col" -> "checkoutStepOption"
      )
    ),
    MPData(SchemaKey(vendor, "content_experiment", format, schemaVersion).toSchemaUri,
      Map("xid"  -> "id", "xvar" -> "variant"))
  )

  // layer of indirection linking fields to schemas
  private val fieldToSchemaMap = contextData
    .flatMap(mpData => mpData.translationTable.keys.map(_ -> mpData.schemaUri))
    .toMap

  /**
   * Converts a CollectorPayload instance into raw events.
   * @param payload The CollectorPaylod containing one or more raw events as collected by
   * a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for schema lookup and validation
   * @return a Validation boxing either a NEL of RawEvents on Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents = {
    val params = toMap(payload.querystring)
    if (params.isEmpty) {
      s"Querystring is empty: no $vendorName event to process".failNel
    } else {
      params.get("t") match {
        case None => s"No $vendorName t parameter provided: cannot determine hit type".failNel
        case Some(hitType) =>
          val contexts     = buildContexts(params, contextData, fieldToSchemaMap)
          val contextJsons = contexts.map(c => buildJsonContext(c._1, c._2))
          val contextParam =
            if (contextJsons.isEmpty) Map.empty
            else Map("co" -> compact(toContexts(contextJsons.toList)))
          for {
            trTable <- unstructEventData.get(hitType).map(_.translationTable)
              .toSuccess(NonEmptyList(s"No matching $vendorName hit type for hit type $hitType"))
            schema  <- lookupSchema(hitType.some, vendorName, unstructEventData.mapValues(_.schemaUri))
            unstructEvent       = buildUnstructEvent(params, trTable, hitType)
            unstructEventParams =
              toUnstructEventParams(protocol, unstructEvent, schema, buildFormatter(), "srv")
          } yield NonEmptyList(RawEvent(
            api         = payload.api,
            parameters  = unstructEventParams ++ contextParam,
            contentType = payload.contentType,
            source      = payload.source,
            context     = payload.context
          ))
      }
    }
  }

  /**
   * Builds an unstruct event by finding the appropriate fields in the original payload and
   * translating them.
   * @param originalParams original payload in key-value format
   * @param translationTable mapping between MP and iglu schema for the specific hit type
   * @param hitType hit type of this unstruct event
   * @return an unstruct event in key-value format
   */
  private def buildUnstructEvent(
    originalParams: Map[String, String],
    translationTable: Map[String, String],
    hitType: String
  ): Map[String, String] =
    originalParams.foldLeft(Map("hitType" -> hitType)) { case (m, (fieldName, value)) =>
      translationTable.get(fieldName).map(newName => m + (newName -> value)).getOrElse(m)
    }

  /**
   * Discovers the contexts in the payload in linear time (size of originalParams).
   * @param originalParams original payload in key-value format
   * @param referenceTable list of context schemas and their associated translation
   * @param fieldToSchemaMap reverse indirection from referenceTable linking fields with the MP
   * nomenclature to schemas
   * @return a map containing the discovered contexts keyed by schema
   */
  private def buildContexts(
    originalParams: Map[String, String],
    referenceTable: List[MPData],
    fieldToSchemaMap: Map[String, String]
  ): Map[String, Map[String, String]] = {
    val refTable = referenceTable.map(d => d.schemaUri -> d.translationTable).toMap
    originalParams.foldLeft(Map.empty[String, Map[String, String]]) {
      case (m, (fieldName, value)) =>
        fieldToSchemaMap.get(fieldName).map { schema =>
          // this is safe when fieldToSchemaMap is built from referenceTable
          val translated = refTable(schema)(fieldName)
          val trTable = m.getOrElse(schema, Map.empty) + (translated -> value)
          m + (schema -> trTable)
        }
        .getOrElse(m)
    }
  }

  private def buildJsonContext(schema: String, fields: Map[String, String]): JValue =
    ("schema" -> schema) ~ ("data" -> fields)
}
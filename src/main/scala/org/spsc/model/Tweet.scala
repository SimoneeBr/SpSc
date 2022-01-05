package org.spsc.model

case class Tweet(in_reply_to_user_id: String,
                 conversation_id: String,
                 source: String,
                 author_id: String,
                 id: String,
                 text: String,
                 entities: Entity,
                 attachments: Attachment,
                 public_metrics: PublicMetric,
                 created_at: String,
                 lang: String,
                 referenced_tweets: Array[ReferencedTweet],
                 reply_settings: String,
                 possibly_sensitive: Boolean,
                 geo: Geo,
                 context_annotations: Array[ContextAnnotation])

case class Meta(newest_id: String,
                oldest_id: String,
                result_count: Int,
                next_token: String)

case class Entity(urls: Array[Url],
                  hashtags: Array[Hashtag],
                  mentions: Array[Mention],
                  annotations: Array[Annotation])

case class Url(start: Int,
               end: Int,
               url: String,
               expanded_url: String,
               display_url: String,
               images: Array[ImageUrl],
               status: Int,
               title: String,
               description: String,
               unwound_url: String)

case class ImageUrl(url: String,
                    width: Int,
                    height: Int)

case class Mention(start: Int,
                   end: Int,
                   username: String,
                   id: String)

case class Annotation(start: Int,
                      end: Int,
                      probability: Double,
                      `type`: String,
                      normalized_text: String)

case class Hashtag(start: Int,
                   end: Int,
                   tag: String)

case class Attachment(media_keys: Array[String])

case class PublicMetric(retweet_count: Int,
                        reply_count: Int,
                        like_count: Int,
                        quote_count: Int)


case class ReferencedTweet(`type`: String, id: String)

case class Geo(place_id: String,
               coordinates: Coordinate)

case class Coordinate(`type`: String,
                      coordinates: Array[Float])

case class ContextAnnotation(domain: Domain,
                             entity: ContextEntity)

case class Domain(id: String,
                  name: String,
                  description: String)

case class ContextEntity(id: String,
                         name: String,
                         description: String)


/* ted5000
 *
 * Copyright (c) 2014 Peter A. Bigot
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * This plugin connects to a TED 5000 energy monitor.  The interface
 * is defined by: http://files.theenergydetective.com/docs/TED5000-API-R330.pdf
 */

#include "collectd.h"
#include "common.h"
#include "plugin.h"

#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <curl/curl.h>
#include <time.h>

typedef enum process_e {
  PROCESS_DEFAULT,              /* default: depends on gauge */
  PROCESS_EXCLUDE,              /* exclude: do not monitor */
  PROCESS_SAMPLE,               /* sample: delegate each sample */
  PROCESS_AGGREGATE,            /* aggregate: provide min, mean, max */
} process_e ;
static const char * const process_str[] = {
  "default", "exclude", "sample", "aggregate"
};

static const char * config_keys[] = {
  "Interval",                  /* Seconds between updates */
  "Hostname",                  /* Host name or IP address of device = "ted5000" */
  "MTU",                       /* MTU on device = 0 */
  "Watts",                     /* Power: Exclude, Sample, Aggregate */
  "Cost",                      /* Cost (USD): Exclude, Sample, Aggregate */
  "Voltage",                   /* Voltage: Exclude, Sample, Aggregate */
};
static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);
static char curl_errstr[CURL_ERROR_SIZE];

static char * host_str = NULL;
#define HOST_S() (host_str ? host_str : "ted5000")
static int mtu = 0;
static int interval_s = -1;
static process_e watts_p = PROCESS_DEFAULT;
static process_e cost_p = PROCESS_DEFAULT;
static process_e voltage_p = PROCESS_DEFAULT;

static char * uri = NULL;
static int curl_flag = 0;
static CURL * curl = NULL;
static int valid_read;

static process_e
decode_process (const char * key,
                const char * value,
                process_e current)
{
  if (0 == strcasecmp(value, "default")) {
    return PROCESS_DEFAULT;
  }
  if (0 == strcasecmp(value, "exclude")) {
    return PROCESS_EXCLUDE;
  }
  if (0 == strcasecmp(value, "sample")) {
    return PROCESS_SAMPLE;
  }
  if (0 == strcasecmp(value, "aggregate")) {
    return PROCESS_AGGREGATE;
  }
  WARNING("ignoring invalid %s process mode: %s", key, value);
  return current;
}

static int
ted5000_config (const char * key,
                const char * value)
{
  if (0 == strcasecmp(key, "Hostname")) {
    if (NULL != host_str) {
      free(host_str);
    }
    host_str = strdup(value);
  } else if (0 == strcasecmp(key, "MTU")) {
    char * ep;
    int v = strtoul(value, &ep, 0);
    if (*ep) {
      WARNING("ted5000 plugin: ignoring invalid MTU: %s", value);
    } else {
      mtu = v;
    }
  } else if (0 == strcasecmp(key, "Interval")) {
    char * ep;
    int v = strtoul(value, &ep, 0);
    if (*ep || (0 >= v)) {
      WARNING("ted5000 plugin: ignoring invalid Interval: %s", value);
    } else {
      interval_s = v;
    }
  } else if (0 == strcasecmp(key, "Watts")) {
    watts_p = decode_process(key, value, watts_p);
  } else if (0 == strcasecmp(key, "Cost")) {
    cost_p = decode_process(key, value, cost_p);
  } else if (0 == strcasecmp(key, "Voltage")) {
    voltage_p = decode_process(key, value, voltage_p);
  } else {
    return -1;
  }
  return 0;
}

#if BYTE_ORDER == BIG_ENDIAN
static uint32_t
le32tohl (uint32_t v)
{
  return (((0xFF & (v >> 24)) << 0)
          | ((0xFF & (v >> 16)) << 8)
          | ((0xFF & (v >> 8)) << 16)
          | ((0xFF & (v >> 0)) << 24));
}
static uint16_t
le16tohs (uint16_t v)
{
  return (((0xFF & (v >> 8)) << 0)
          | ((0xFF & (v >> 0)) << 8));
}
#else /* byte order */
static uint32_t
le32tohl (uint32_t v)
{
  return v;
}
static uint16_t
le16tohs (uint16_t v)
{
  return v;
}
#endif /* byte order */

/** Length of one record as received on-wire.  This is less than
 * sizeof(ted5000_sec_ty) due to padding. */
#define TED5000_SEC_TY_PACKED_SIZE 16

/** Length in octets of the base64-encoded on-wire sample.  This is
 * larger than the record length because it needs to align to 24 bits
 * (four chars). */
#define TED5000_SEC_TY_BASE64_SIZE (3 * ((TED5000_SEC_TY_PACKED_SIZE + 2) / 3))

typedef struct sample_ty {
  uint32_t time_posix;          /**< The time of the observation in the POSIX epoch */
  uint32_t power_W;             /**< Power use at the observation, in watts */
  uint32_t cost_ct;             /**< Estimated cost of the observation, in 100*USD */
  uint16_t voltage_dV;          /**< Observed split-phase voltage, in volts */
} sample_ty;

/** Used to sort the retrieved observations */
static int
cmp_samples (const void * p1,
             const void * p2)
{
  const sample_ty * s1 = (const sample_ty *)p1;
  const sample_ty * s2 = (const sample_ty *)p2;
  if (s1->time_posix < s2->time_posix) {
    return -1;
  }
  return (s1->time_posix > s2->time_posix);
}

static sample_ty * samples;
static unsigned int num_samples;
static unsigned int max_samples;

static time_t last_time = 0;

static size_t
curl_consume (void * vbuffer,
              size_t size,
              size_t nmemb,
              void * userp)
{
  size_t nbytes = nmemb * size;
  const unsigned char * sp = (const unsigned char *)vbuffer;
  const unsigned char * const esp = sp + nbytes;
  unsigned char bytes[TED5000_SEC_TY_BASE64_SIZE];
  uint32_t u32;
  uint16_t u16;
  unsigned char * dp;
  (void)userp;

  /* TED5000 data through this interface is a series of Base-64
   * encoded observations, one per line.  The data include the time of
   * the sample, and the measurements. */
  while (sp < esp) {
    unsigned int bits = 0;
    int num_bits = 0;
    dp = bytes;
    while (('\n' != *sp) && (sp < esp)) {
      const unsigned char ch = *sp++;
      bits = bits << 6;
      if (('A' <= ch) && (ch <= 'Z')) {
        bits |= (unsigned int)ch - 'A';
      } else if (('a' <= ch) && (ch <= 'z')) {
        bits |= 26 + (unsigned int)ch - 'a';
      } else if (('0' <= ch) && (ch <= '9')) {
        bits |= 52 + (unsigned int)ch - '0';
      } else if ('+' == ch) {
        bits |= 62;
      } else if ('/' == ch) {
        bits |= 63;
      } else if ('=' == ch) {
        /* padding */
      } else {
        fprintf(stderr, "ERROR: invalid base64 character '%c' (0x%02x)\n", ch, ch);
        return -1;
      }
      num_bits += 6;
      if (24 == num_bits) {
        *dp++ = (bits >> 16);
        *dp++ = (bits >> 8);
        *dp++ = bits;
        num_bits = 0;
      }
    }
    if (num_bits != 0) {
      fprintf(stderr, "ERROR: bad decode\n");
      return -1;
    }
    if (num_samples == max_samples) {
      max_samples += (max_samples / 2) + 10;
      samples = realloc(samples, max_samples * sizeof(*samples));
    }
    {
      struct tm tms;
      sample_ty * op = samples + num_samples;

      /* Decode for secondhistory format. */
      dp = bytes;
      memset(&tms, 0, sizeof(tms));
      tms.tm_year = 100 + *dp++;
      tms.tm_mon = *dp++ - 1;
      tms.tm_mday = *dp++;
      tms.tm_hour = *dp++;
      tms.tm_min = *dp++;
      tms.tm_sec = *dp++;
      op->time_posix = (unsigned int)mktime(&tms);

      /* Make curl stop once we've reached data when already seen, so
       * we don't overload the gateway.  Inhibit any error diagnostics
       * in this case. */
      if (op->time_posix <= last_time) {
        valid_read = 1;
        return -1;
      }
      ++num_samples;
      memcpy(&u32, dp, sizeof(u32));
      dp += sizeof(u32);
      op->power_W = le32tohl(u32);
      memcpy(&u32, dp, sizeof(u32));
      dp += sizeof(u32);
      op->cost_ct = le32tohl(u32);
      memcpy(&u16, dp, sizeof(u16));
      dp += sizeof(u16);
      op->voltage_dV = le16tohs(u16);
    }
    if (sp < esp) {
      ++sp;
    }
  }
  return nmemb;
}

typedef struct aggregation_ty {
  int samples;
  gauge_t sum;
  gauge_t low;
  gauge_t high;
} aggregation_ty;

static void
add_aggregation (aggregation_ty * ap,
                 gauge_t sample)
{
  if (0 == ap->samples) {
    ap->sum = ap->low = ap->high = sample;
  } else {
    ap->sum += sample;
    if (sample < ap->low) {
      ap->low = sample;
    }
    if (sample > ap->high) {
      ap->high = sample;
    }
  }
  ap->samples += 1;
}

static void
add_sample (value_list_t * vlp,
            const char * type_str,
            process_e mode,
            gauge_t sample,
            aggregation_ty * ap)
{
  switch (mode) {
    default:
    case PROCESS_EXCLUDE:
      return;
    case PROCESS_SAMPLE:
      vlp->values_len = 1;
      vlp->values[0].gauge = sample;
      sstrncpy(vlp->type, type_str, sizeof(vlp->type));
      DEBUG("ted5000 plugin: sample %s at %u",
            type_str, (unsigned int)CDTIME_T_TO_TIME_T(vlp->time));
      plugin_dispatch_values(vlp);
      break;
    case PROCESS_AGGREGATE:
      add_aggregation(ap, sample);
      break;
  }
}

static void
flush_samples (value_list_t * vlp,
               const char * type_str,
               process_e mode,
               aggregation_ty * ap)
{
  if ((PROCESS_AGGREGATE != mode) || (0 == ap->samples)) {
    return;
  }
  vlp->values_len = 3;
  vlp->values[0].gauge = ap->low;
  vlp->values[1].gauge = ap->sum / ap->samples;
  vlp->values[2].gauge = ap->high;
  sstrncpy(vlp->type, type_str, sizeof(vlp->type));
  DEBUG("ted5000 plugin: aggregation %s with %u at %u",
        type_str, ap->samples, (unsigned int)CDTIME_T_TO_TIME_T(vlp->time));
  plugin_dispatch_values(vlp);
}

static int
ted5000_read ()
{
  CURLcode cc;

  num_samples = 0;
  valid_read = 0;
  cc = curl_easy_perform(curl);
  if ((CURLE_OK != cc) && (! valid_read)) {
    ERROR("ted5000_plugin: curl_easy_perform failed: %s", curl_errstr);
    return -1;
  }

  if (0 < num_samples) {
    int si;
    value_t values[3];
    value_list_t vl = VALUE_LIST_INIT;
    aggregation_ty watts;
    aggregation_ty cost;
    aggregation_ty voltage;

    memset(&watts, 0, sizeof(watts));
    memset(&cost, 0, sizeof(cost));
    memset(&voltage, 0, sizeof(voltage));

    vl.values = values;
    sstrncpy(vl.host, HOST_S(), sizeof(vl.host));
    sstrncpy(vl.plugin, "ted5000", sizeof(vl.plugin));
    ssnprintf(vl.plugin_instance, sizeof(vl.plugin_instance),
              "MTU%u", mtu);
    vl.interval = TIME_T_TO_CDTIME_T(interval_s);

    qsort(samples, num_samples, sizeof(*samples), cmp_samples);
    DEBUG("ted5000 plugin: %u samples after %u as %s",
          num_samples, (unsigned int)last_time, vl.plugin_instance);
    for (si = 0; si < num_samples; ++si) {
      const sample_ty * sp = samples + si;
      /* Sometimes the device puts two samples at the same time,
       * which is not acceptable to rrd.  Ignore those. */
      if (last_time != sp->time_posix) {
        if (0 == last_time) {
          /* Special case on startup: let collectd create the
           * databases before we attempt to store multiple samples in
           * it (viz. with PROCESS_SAMPLE).  The remaining data will
           * be re-examined on the next read. */
          si = num_samples-1;
        }
        last_time = sp->time_posix;
        vl.time = TIME_T_TO_CDTIME_T(last_time);
        add_sample(&vl, "watts", watts_p, sp->power_W, &watts);
        add_sample(&vl, "cost", cost_p, sp->cost_ct / 100.0, &cost);
        add_sample(&vl, "voltage", voltage_p, sp->voltage_dV / 20.0, &voltage);
      } else {
        INFO("ted5000 plugin: duplicate at %u: %u %u\n",
             (unsigned int)sp->time_posix,
             (sp-1)->power_W, sp->power_W);
      }
    }
    flush_samples(&vl, "watts_span", watts_p, &watts);
    flush_samples(&vl, "cost_span", cost_p, &cost);
    flush_samples(&vl, "voltage_span", voltage_p, &voltage);
  }

  return 0;
}

static int
ted5000_shutdown (void)
{
  if (NULL != curl) {
    curl_easy_cleanup(curl);
    curl = NULL;
  }
  if (curl_flag) {
    curl_global_cleanup();
    curl_flag = 0;
  }
  if (NULL != uri) {
    free(uri);
    uri = NULL;
  }
  if (NULL != samples) {
    free(samples);
    max_samples = num_samples = 0;
  }
  return 0;
}

static int
ted5000_init ()
{
  char uri_buffer[1024];
  int rc = 0;
  CURLcode cc;

  last_time = 0;
  max_samples = 600;
  num_samples = 0;
  samples = malloc(max_samples * sizeof(*samples));
  if (! samples) {
    WARNING("ted5000 plugin: unable to allocate sample buffer");
    return -1;
  }

  if (PROCESS_DEFAULT == watts_p) {
    watts_p = PROCESS_AGGREGATE;
  }
  if (PROCESS_DEFAULT == cost_p) {
    cost_p = PROCESS_EXCLUDE;
  }
  if (PROCESS_DEFAULT == voltage_p) {
    voltage_p = PROCESS_AGGREGATE;
  }

  if (NULL != uri) {
    free(uri);
    uri = NULL;
  }
  rc = ssnprintf(uri_buffer, sizeof(uri_buffer),
                 "http://%s/history/rawsecondhistory.raw?MTU=%u",
                 HOST_S(), mtu);
  if (0 > rc) {
    WARNING("ted5000 plugin: unable to cache URI");
    return -1;
  }
  uri = strdup(uri_buffer);

  cc = curl_global_init(CURL_GLOBAL_NOTHING);
  if (0 != cc) {
    WARNING("ted5000 plugin: curl_global_init failed");
    return -1;
  }
  curl_flag = 1;

  rc = 0;
  do {
    curl = curl_easy_init();
    if (NULL == curl) {
      WARNING("ted5000 plugin: failed to initialize curl");
      rc = -1;
      break;
    }
    cc = curl_easy_setopt(curl, CURLOPT_URL, uri);
    if (CURLE_OK == cc) {
      cc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_consume);
    }
    if (CURLE_OK == cc) {
      cc = curl_easy_setopt (curl, CURLOPT_USERAGENT, PACKAGE_NAME"/"PACKAGE_VERSION);
    }
    if (CURLE_OK == cc) {
      cc = curl_easy_setopt (curl, CURLOPT_ERRORBUFFER, curl_errstr);
    }
    if (CURLE_OK != cc) {
      WARNING("ted5000 plugin: failed to configure curl");
      rc = -1;
      break;
    }
  } while (0);

  if (0 == rc) {
    struct timespec interval;
    struct timespec * intervalp = NULL;

    if (0 < interval_s) {
      memset(&interval, 0, sizeof(interval));
      interval.tv_sec = interval_s;
      intervalp = &interval;
    } else {
      interval_s = (int)CDTIME_T_TO_TIME_T(cf_get_default_interval());
    }
    NOTICE("ted5000 plugin: interval %u watts=%s cost=%s voltage=%s",
           interval_s, process_str[watts_p], process_str[cost_p], process_str[voltage_p]);
    plugin_register_complex_read("ged5000", "ted5000", ted5000_read, intervalp, NULL);
  }

  if (0 != rc) {
    ted5000_shutdown();
  }
  return rc;
}

void
module_register (void)
{
  plugin_register_config("ted5000", ted5000_config, config_keys, config_keys_num);
  plugin_register_init("ted5000", ted5000_init);
  plugin_register_shutdown("ted5000", ted5000_shutdown);
}

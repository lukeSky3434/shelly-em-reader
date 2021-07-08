package luke.sky.shelly;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.glassfish.jersey.client.ClientProperties;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author pendl2
 */
public class EnergyMeterReader implements Runnable
{
	private static final String SHELLY_PATH = "status";

	private static final String ENV_LOG_LEVEL = "LOG.LEVEL";
	private static final String ENV_POLL_TIME = "POLL.TIME.MS"; // default 10000
	private static final String ENV_SHELLY_HOST = "SHELLY.HOST";

	private final Client client;
	private final WebTarget webTarget;
	private static final Logger LOG = LogManager.getLogger(EnergyMeterReader.class);
	private long pollTime;

	private String shellyHost;

	private void initialize()
	{
		Configurator.initialize(new DefaultConfiguration());
		String level = System.getenv(ENV_LOG_LEVEL);
		Level ll = Level.getLevel(level == null ? Level.INFO.name() : level);
		Configurator.setRootLevel(ll);

		final long defaultMs = 10000L;
		String timeMs = System.getenv(ENV_POLL_TIME);
		try {
			pollTime = Long.parseLong(timeMs);
		}
		catch (NumberFormatException x) {
			LOG.warn("<{}> is not a number, taking default value <{}>", timeMs, defaultMs);
			pollTime = defaultMs;
		}

		shellyHost = System.getenv(ENV_SHELLY_HOST);
		if (shellyHost == null || shellyHost.isEmpty()) {
			shellyHost = "shelly-3em";
		}


		if (Objects.isNull(shellyHost)) {
			throw new ExceptionInInitializerError(ENV_SHELLY_HOST + " must be set");
		}
	}

	public EnergyMeterReader()
	{
		initialize();

		client = ClientBuilder.newClient();
		client.property(ClientProperties.CONNECT_TIMEOUT, 2000);
		client.property(ClientProperties.READ_TIMEOUT, 2000);

		LOG.info("initialization done");

		webTarget = client
			.target(UriBuilder.fromUri("http://" + shellyHost)
				.path(SHELLY_PATH)
				.build());
	}

	public static void main(String[] args) throws Exception
	{
		try {
			Thread th = new Thread(new EnergyMeterReader());
			th.setName("ReaderThread");
			th.start();
		}
		catch (Exception x) {
			LOG.error("caught exception", x);
			throw x;
		}
	}

	@Override
	public void run()
	{
		DbAdapter dbAdpater = new InfluxAdapter();

		boolean firstRound = true;
		Double oldTotalConsumed = 0.0;
		Double oldTotalReturned = 0.0;
		while (true) {
			LOG.info("doing webservice call - uri <" + webTarget.getUri() + ">");
			try (Response response = webTarget
				.request()
				.get()) {
				String entity = response.readEntity(String.class);

				JSONObject obj = new JSONObject(entity);
				JSONArray emeters = obj.getJSONArray("emeters");

				Double totalPower = 0.0;
				Double totalReturned = 0.0;
				Double totalConsumed = 0.0;
				List<Dimension> dim = new ArrayList<>();
				for (int i = 0; i < emeters.length(); i++) {
					JSONObject item = emeters.getJSONObject(i);
					if (Boolean.TRUE.equals(item.get("is_valid"))) {
						for (String k : item.keySet()) {
							if(LOG.isDebugEnabled()) {
							  LOG.debug("meter-{} => {}={}", i, k, item.get(k));
							}
							if ("power".equalsIgnoreCase(k)) {
								totalPower += item.getDouble(k);
							}
							else if ("total_returned".equalsIgnoreCase(k)) {
								totalReturned += item.getDouble(k);

							}
							else if ("total".equalsIgnoreCase(k)) {
								totalConsumed += item.getDouble(k);
							}
						}
					}
					else {
						LOG.warn("meter {} is not functioning correctly", i);
					}
				}
				if (!firstRound) {
					if(LOG.isDebugEnabled()) {
						LOG.debug("total => power={} Watt", totalPower);
						LOG.debug("total => totalConsumed={} kWh", totalConsumed);
						LOG.debug("total => totalReturned={} kWh", totalReturned);
					}
					double diffConsumed = (totalConsumed - oldTotalConsumed) * 1000;
					double diffReturned = (totalReturned - oldTotalReturned) * 1000;
					LOG.info("total => totalConsumed-diff={} Wh, total={} kWh", diffConsumed, totalConsumed);
					LOG.info("total => totalReturned-diff={} Wh, total={} kWh", diffReturned, totalReturned);

					dim.add(new Dimension("unit", "Wh"));
					dim.add(new Dimension("area", "home"));
					dim.add(new Dimension("equipment", "shelly-3em"));
					if(diffConsumed > 0.0) {
					  dbAdpater.sendMeasurement("energy", "energy_consumed", totalReturned.longValue(), dim);
					}
					if(diffReturned > 0.0) {
					  dbAdpater.sendMeasurement("energy", "energy_returned", totalConsumed.longValue(), dim);
					}
				}

				oldTotalConsumed = totalConsumed;
				oldTotalReturned = totalReturned;

			}
			catch (Exception x) {
				LOG.error("error while processing data", x);
			}
			try {
				long sleep = pollTime;
				if (firstRound) {
					sleep = 500;
					firstRound = false;
				}
				LOG.info("sleep {} milliseconds ...", sleep);

				Thread.sleep(sleep);
			}
			catch (Exception e) {
				LOG.error("error while sleeping", e);
			}
		}
	}
}

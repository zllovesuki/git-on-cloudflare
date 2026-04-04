import journal from "./meta/_journal.json";
import m0000 from "./0000_natural_lyja.sql";
import m0001 from "./0001_icy_ulik.sql";
import m0002 from "./0002_ambiguous_ares.sql";
import m0003 from "./0003_abandoned_whirlwind.sql";

export default {
  journal,
  migrations: {
    m0000,
    m0001,
    m0002,
    m0003,
  },
};

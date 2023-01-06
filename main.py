from SportsBettingEngine import BettingEngine

if __name__ == "__main__":
    b = BettingEngine()
    b.run_engine()
    b.odds_portal.web.close()

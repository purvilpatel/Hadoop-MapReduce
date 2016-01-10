class Player {
	private final static String WHITE_WON = "1-0";
	private final static String BLACK_WON = "0-1";
	private final static String GAME_DRAW = "1/2-1/2";
	String playerId;
	String playerColor;
	int win;
	int lost;
	int draw;

	/**
	 * @param playerId
	 *            the playerId to set
	 */
	public void setPlayerId(String playerId) {
		this.playerId = playerId;
	}

	/**
	 * @param playerColor
	 *            the playerColor to set
	 */
	public void setPlayerColor(String playerColor) {
		this.playerColor = playerColor;
	}

	/**
	 * @param win
	 *            the win to set
	 */
	public void setWin(int win) {
		this.win = win;
	}

	/**
	 * @param loss
	 *            the loss to set
	 */
	public void setLoss(int lost) {
		this.lost = lost;
	}

	/**
	 * @param draw
	 *            the draw to set
	 */
	public void setDraw(int draw) {
		this.draw = draw;
	}

	/**
	 * @param lost
	 *            the lost to set
	 */
	public void setLost(int lost) {
		this.lost = lost;
	}

	/**
	 * @return the playerId
	 */
	public String getPlayerId() {
		return playerId;
	}

	/**
	 * @return the playerColor
	 */
	public String getPlayerColor() {
		return playerColor;
	}

	/**
	 * @return the win
	 */
	public int getWin() {
		return win;
	}

	/**
	 * @return the lost
	 */
	public int getLost() {
		return lost;
	}

	/**
	 * @return the draw
	 */
	public int getDraw() {
		return draw;
	}

	public String toString() {
		return playerId + " " + playerColor + " " + Integer.toString(win) + " "
				+ Integer.toString(lost) + " " + Integer.toString(draw);
	}

	public String getPlayerNameColor() {
		return playerId + " " + playerColor + " ";
	}

	public String getPlayerGameCount() {
		return Integer.toString(win) + " "
				+ Integer.toString(lost) + " " + Integer.toString(draw);
	}


	public void setResult(String text) {
		if (playerColor == "White") {
			switch (text) {
			case BLACK_WON:
				lost = 1;
				break;
			case WHITE_WON:
				win = 1;
				break;
			case GAME_DRAW:
				draw = 1;
				break;
			}
		} else if (playerColor == "Black") {
			switch (text) {
			case BLACK_WON:
				win = 1;
				break;
			case WHITE_WON:
				lost = 1;
				break;
			case GAME_DRAW:
				draw = 1;
				break;
			}
		}
	}

	public void toPlayer(String str) {
		String value[] = str.split(" ");
		playerId = value[0];
		playerColor = value[1];
		win = Integer.parseInt(value[2]);
		lost = Integer.parseInt(value[3]);
		draw = Integer.parseInt(value[4]);
	}
}


use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RewardsClaimed {
    pub root: [u8; 32],
    pub earner: String,
    pub claimer: String,
    pub recipient: String,
    pub token: String,
    #[serde(rename = "claimedAmount")]
    pub claimed_amount: u128,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewards_claimed_deserialization() {
        let root_bytes_hex = "01";
        let rewards_claimed = r#"{
            "root": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            "earner": "0x124",
            "claimer": "0x125",
            "recipient": "0x126",
            "token": "0x127",
            "claimedAmount": 127
        }"#;
        let rewards_claimed: RewardsClaimed = serde_json::from_str(rewards_claimed).unwrap();
        let root_bytes = hex::decode(root_bytes_hex).unwrap();
        let root_bytes = [0]
            .repeat(31)
            .to_vec()
            .iter()
            .chain(root_bytes.iter())
            .copied()
            .collect::<Vec<u8>>();
        assert_eq!(rewards_claimed.root.to_vec(), root_bytes);
        assert_eq!(rewards_claimed.earner, "0x124");
        assert_eq!(rewards_claimed.claimer, "0x125");
        assert_eq!(rewards_claimed.recipient, "0x126");
        assert_eq!(rewards_claimed.token, "0x127");
        assert_eq!(rewards_claimed.claimed_amount, 127);
    }
}

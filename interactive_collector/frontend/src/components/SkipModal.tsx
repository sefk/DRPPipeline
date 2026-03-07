/**
 * SkipModal - Asks for a reason, then saves project with status "collector_hold - {reason}".
 */
import { useState } from "react";
import { useCollectorStore } from "../store";

export function SkipModal() {
  const { skipModalOpen, closeSkipModal, skip } = useCollectorStore();
  const [reason, setReason] = useState("");

  if (!skipModalOpen) return null;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (reason.trim()) {
      skip(reason.trim());
      setReason("");
    }
  };

  const handleCancel = () => {
    setReason("");
    closeSkipModal();
  };

  return (
    <div className="save-modal show" role="dialog" aria-label="Skip reason">
      <div className="save-modal-dialog">
        <strong>Skip</strong>
        <form onSubmit={handleSubmit}>
          <label htmlFor="skip-reason">Reason</label>
          <input
            id="skip-reason"
            type="text"
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="e.g. waiting on requester"
            className="save-modal-input"
            autoFocus
          />
          <div className="save-modal-actions">
            <button type="button" className="save-modal-ok" onClick={handleCancel}>
              Cancel
            </button>
            <button type="submit" className="save-modal-ok" disabled={!reason.trim()}>
              Submit
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

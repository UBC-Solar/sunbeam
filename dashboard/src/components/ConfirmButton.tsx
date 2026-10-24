import {useEffect, useRef, useState} from "react";

type ConfirmButtonProps = {
  label: string;
  confirmLabel?: string;
  onConfirm: () => void;
  disabled?: boolean;
  armedForMs?: number;
  className?: string;
};

export function ConfirmButton({
  label,
  confirmLabel = "Confirm?",
  onConfirm,
  disabled,
  armedForMs = 3000,
  className,
}: ConfirmButtonProps) {
  const [armed, setArmed] = useState(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, []);

  function handleClick() {
    if (!armed) {
      setArmed(true);
      timeoutRef.current = setTimeout(() => setArmed(false), armedForMs);
      return;
    }

    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    setArmed(false);
    onConfirm();
  }

  return (
    <button
      className={`${className ?? ""} ${armed ? "danger" : ""}`.trim()}
      disabled={disabled}
      onClick={handleClick}
    >
      {armed ? confirmLabel : label}
    </button>
  );
}